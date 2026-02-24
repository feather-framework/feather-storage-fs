//
//  StorageClientFS.swift
//  feather-storage-fs
//
//  Created by Tibor Bödecs on 2023. 01. 16.

import FeatherStorage
import NIOCore
import SystemPackage
import _NIOFileSystem

/// Filesystem-backed storage driver implemented via NIO FileSystem APIs.
public struct StorageClientFS: StorageClient {
    private let fileSystem: FileSystem
    private let rootPath: String
    private let chunkSize: Int

    /// Creates a filesystem-backed storage client.
    ///
    /// - Parameters:
    ///   - rootPath: The root directory used as the storage namespace.
    ///   - chunkSize: The maximum number of bytes read or written per chunk.
    ///   - fileSystem: The filesystem implementation used for I/O operations.
    public init(
        rootPath: String,
        chunkSize: Int = 64 * 1024,
        fileSystem: FileSystem = .shared
    ) {
        self.rootPath = Self.trimTrailingSlashes(rootPath)
        self.chunkSize = chunkSize
        self.fileSystem = fileSystem
    }

    /// Uploads an object for the given storage key.
    ///
    /// - Parameters:
    ///   - key: The object key relative to `rootPath`.
    ///   - sequence: The byte sequence to persist.
    /// - Throws: `StorageClientError` when the key is invalid or I/O fails.
    public func upload(
        key: String,
        sequence: StorageSequence
    ) async throws(StorageClientError) {
        let destination = try resolvePath(for: key)
        let parent = parentPath(of: destination)

        do {
            do {
                try await fileSystem.createDirectory(
                    at: parent,
                    withIntermediateDirectories: true
                )
            }
            catch let error as FileSystemError
            where error.code == .fileAlreadyExists {
                _ = error
            }

            _ = try? await fileSystem.removeItem(
                at: destination,
                strategy: .platformDefault,
                recursively: false
            )

            try await fileSystem.withFileHandle(
                forWritingAt: destination,
                options: .newFile(replaceExisting: true)
            ) { fileHandle in
                var offset: Int64 = 0

                for try await chunk in sequence {

                    try await fileHandle.write(
                        contentsOf: chunk.readableBytesView,
                        toAbsoluteOffset: offset
                    )
                    offset += Int64(chunk.readableBytes)
                }
            }
        }
        catch let error as StorageClientError {
            throw error
        }
        catch {
            throw .unknown(error)
        }
    }

    /// Downloads an object or byte range for a storage key.
    ///
    /// - Parameters:
    ///   - key: The object key relative to `rootPath`.
    ///   - range: An optional inclusive byte range to read.
    /// - Returns: A storage sequence containing the requested bytes.
    /// - Throws: `StorageClientError` when the key, range, or I/O operation is invalid.
    public func download(
        key: String,
        range: ClosedRange<Int>?
    ) async throws(StorageClientError) -> StorageSequence {
        let path = try resolvePath(for: key)

        do {
            guard let info = try await fileSystem.info(forFileAt: path) else {
                throw StorageClientError.invalidKey
            }

            let fileSize = Int64(info.size)
            guard fileSize > 0 else {
                return .init(
                    asyncSequence: ByteBufferSequence(buffer: .init()),
                    length: 0
                )
            }

            let start = Int64(range?.lowerBound ?? 0)
            let end = Int64(range?.upperBound ?? (Int(fileSize) - 1))
            guard start >= 0, end >= start, end < fileSize else {
                throw StorageClientError.invalidBuffer
            }

            let stream = AsyncThrowingStream<ByteBuffer, Error> {
                continuation in
                let task = Task {
                    do {
                        try await fileSystem.withFileHandle(
                            forReadingAt: path
                        ) { fileHandle in
                            for try await chunk in fileHandle.readChunks(
                                in: start...end,
                                chunkLength: .bytes(Int64(chunkSize))
                            ) {
                                continuation.yield(chunk)
                            }
                        }
                        continuation.finish()
                    }
                    catch {
                        continuation.finish(throwing: error)
                    }
                }
                continuation.onTermination = { _ in
                    task.cancel()
                }
            }

            return StorageSequence(
                asyncSequence: stream,
                length: UInt64(end - start + 1)
            )
        }
        catch let error as StorageClientError {
            throw error
        }
        catch {
            throw .unknown(error)
        }
    }

    /// Checks whether an object exists for a storage key.
    ///
    /// - Parameter key: The object key relative to `rootPath`.
    /// - Returns: `true` if the object exists, otherwise `false`.
    /// - Throws: `StorageClientError` when filesystem metadata lookup fails.
    public func exists(
        key: String
    ) async throws(StorageClientError) -> Bool {
        guard let path = try? resolvePath(for: key) else {
            return false
        }
        do {
            return (try await fileSystem.info(forFileAt: path)) != nil
        }
        catch let error as StorageClientError {
            throw error
        }
        catch {
            throw .unknown(error)
        }
    }

    /// Returns the size of an object in bytes.
    ///
    /// - Parameter key: The object key relative to `rootPath`.
    /// - Returns: The file size in bytes, or `0` for missing/non-regular files.
    /// - Throws: `StorageClientError` when metadata lookup fails.
    public func size(
        key: String
    ) async throws(StorageClientError) -> UInt64 {
        do {
            let path = try resolvePath(for: key)
            guard let info = try await fileSystem.info(forFileAt: path) else {
                return 0
            }
            return info.type == FileType.regular ? UInt64(info.size) : 0
        }
        catch let error as StorageClientError {
            throw error
        }
        catch {
            throw .unknown(error)
        }
    }

    /// Copies an object from one key to another key.
    ///
    /// - Parameters:
    ///   - source: The source object key.
    ///   - destination: The destination object key.
    /// - Throws: `StorageClientError` when the source is missing or I/O fails.
    public func copy(
        key source: String,
        to destination: String
    ) async throws(StorageClientError) {
        let sourcePath = try resolvePath(for: source)
        let destinationPath = try resolvePath(for: destination)
        let destinationParent = parentPath(of: destinationPath)

        do {
            guard try await fileSystem.info(forFileAt: sourcePath) != nil else {
                throw StorageClientError.invalidKey
            }

            try await fileSystem.createDirectory(
                at: destinationParent,
                withIntermediateDirectories: true
            )

            _ = try? await fileSystem.removeItem(
                at: destinationPath,
                strategy: .platformDefault,
                recursively: true
            )

            try await fileSystem.copyItem(at: sourcePath, to: destinationPath)
        }
        catch let error as StorageClientError {
            throw error
        }
        catch {
            throw .unknown(error)
        }
    }

    /// Lists direct child entries for a directory key.
    ///
    /// - Parameter key: The directory key to inspect. `nil` lists the root directory.
    /// - Returns: Sorted child entry names.
    /// - Throws: `StorageClientError` when the key is invalid or listing fails.
    public func list(
        key: String?
    ) async throws(StorageClientError) -> [String] {
        let directory = try resolvePath(for: key ?? "")

        do {
            guard let info = try await fileSystem.info(forFileAt: directory)
            else {
                return []
            }
            guard info.type == FileType.directory else {
                return []
            }

            return try await fileSystem.withDirectoryHandle(atPath: directory) {
                handle in
                var result: [String] = []
                for try await entry in handle.listContents() {
                    result.append(entry.name.string)
                }
                result.sort()
                return result
            }
        }
        catch {
            throw .unknown(error)
        }
    }

    /// Deletes an object or directory tree for the given key.
    ///
    /// - Parameter key: The object or directory key to remove.
    /// - Throws: `StorageClientError` when deletion fails.
    public func delete(
        key: String
    ) async throws(StorageClientError) {
        let path = try resolvePath(for: key)

        do {
            guard try await fileSystem.info(forFileAt: path) != nil else {
                return
            }
            _ = try await fileSystem.removeItem(
                at: path,
                strategy: .platformDefault,
                recursively: true
            )
        }
        catch {
            throw .unknown(error)
        }
    }

    /// Creates a directory for the given key.
    ///
    /// - Parameter key: The directory key to create.
    /// - Throws: `StorageClientError` when directory creation fails.
    public func create(
        key: String
    ) async throws(StorageClientError) {
        let directory = try resolvePath(for: key)

        do {
            try await fileSystem.createDirectory(
                at: directory,
                withIntermediateDirectories: true
            )
        }
        catch {
            throw .unknown(error)
        }
    }

    /// Creates a multipart upload identifier for a key.
    ///
    /// - Parameter key: The destination object key for the multipart upload.
    /// - Returns: A newly reserved multipart upload identifier.
    /// - Throws: `StorageClientError` when identifier reservation fails.
    public func createMultipartId(
        key: String
    ) async throws(StorageClientError) -> String {
        do {
            for _ in 0..<16 {
                let uploadId = randomIdentifier(prefix: "upload")
                let uploadDirectory = try multipartDirectory(
                    for: key,
                    uploadId: uploadId
                )
                do {
                    try await fileSystem.createDirectory(
                        at: uploadDirectory,
                        withIntermediateDirectories: true
                    )
                    return uploadId
                }
                catch let error as FileSystemError
                where error.code == .fileAlreadyExists {
                    _ = error
                    continue
                }
            }
            throw StorageClientError.invalidBuffer
        }
        catch let error as StorageClientError {
            throw error
        }
        catch {
            throw .unknown(error)
        }
    }

    /// Uploads a single multipart chunk.
    ///
    /// - Parameters:
    ///   - multipartId: The multipart upload identifier.
    ///   - key: The destination object key.
    ///   - number: The 1-based chunk number.
    ///   - sequence: The chunk payload.
    /// - Returns: Metadata describing the uploaded chunk.
    /// - Throws: `StorageClientError` when multipart state is invalid or I/O fails.
    public func upload(
        multipartId: String,
        key: String,
        number: Int,
        sequence: StorageSequence
    ) async throws(StorageClientError) -> StorageMultipartChunk {
        guard number > 0 else {
            throw .invalidMultipartChunk
        }

        let uploadDirectory = try multipartDirectory(
            for: key,
            uploadId: multipartId
        )

        do {
            guard try await fileSystem.info(forFileAt: uploadDirectory) != nil
            else {
                throw StorageClientError.invalidMultipartId
            }
            let chunkId = randomIdentifier(prefix: "chunk")
            let partKey =
                ".multipart/\(key)/\(multipartId)/\(number)-\(chunkId).part"
            try await upload(key: partKey, sequence: sequence)
            return .init(id: chunkId, number: number)
        }
        catch {
            if let error = error as? StorageClientError {
                throw error
            }
            throw .unknown(error)
        }
    }

    /// Aborts an in-progress multipart upload and removes staged chunks.
    ///
    /// - Parameters:
    ///   - multipartId: The multipart upload identifier.
    ///   - key: The destination object key.
    /// - Throws: `StorageClientError` when the multipart upload is invalid or cleanup fails.
    public func abort(
        multipartId: String,
        key: String
    ) async throws(StorageClientError) {
        let uploadDirectory = try multipartDirectory(
            for: key,
            uploadId: multipartId
        )

        do {
            guard try await fileSystem.info(forFileAt: uploadDirectory) != nil
            else {
                throw StorageClientError.invalidMultipartId
            }

            _ = try await fileSystem.removeItem(
                at: uploadDirectory,
                strategy: .platformDefault,
                recursively: true
            )
        }
        catch let error as StorageClientError {
            throw error
        }
        catch {
            throw .unknown(error)
        }
    }

    /// Finalizes a multipart upload by assembling all uploaded chunks.
    ///
    /// - Parameters:
    ///   - multipartId: The multipart upload identifier.
    ///   - key: The destination object key.
    ///   - chunks: The chunks to merge into the final object.
    /// - Throws: `StorageClientError` when validation or file assembly fails.
    public func finish(
        multipartId: String,
        key: String,
        chunks: [StorageMultipartChunk]
    ) async throws(StorageClientError) {
        let uploadDirectory = try multipartDirectory(
            for: key,
            uploadId: multipartId
        )
        let destination = try resolvePath(for: key)
        let destinationParent = parentPath(of: destination)

        do {
            guard try await fileSystem.info(forFileAt: uploadDirectory) != nil
            else {
                throw StorageClientError.invalidMultipartId
            }

            try await fileSystem.createDirectory(
                at: destinationParent,
                withIntermediateDirectories: true
            )

            _ = try? await fileSystem.removeItem(
                at: destination,
                strategy: .platformDefault,
                recursively: false
            )

            try await fileSystem.withFileHandle(
                forWritingAt: destination,
                options: .newFile(replaceExisting: true)
            ) { writeHandle in
                var offset: Int64 = 0

                for chunk in chunks.sorted(by: { $0.number < $1.number }) {
                    let partPath = try resolvePath(
                        for:
                            ".multipart/\(key)/\(multipartId)/\(chunk.number)-\(chunk.id).part"
                    )

                    guard
                        let partInfo = try await fileSystem.info(
                            forFileAt: partPath
                        )
                    else {
                        throw StorageClientError.invalidMultipartChunk
                    }

                    try await fileSystem.withFileHandle(forReadingAt: partPath)
                    { readHandle in
                        for try await partChunk in readHandle.readChunks(
                            in: 0..<Int64(partInfo.size),
                            chunkLength: .bytes(Int64(chunkSize))
                        ) {
                            try await writeHandle.write(
                                contentsOf: partChunk.readableBytesView,
                                toAbsoluteOffset: offset
                            )
                            offset += Int64(partChunk.readableBytes)
                        }
                    }
                }
            }

            _ = try await fileSystem.removeItem(
                at: uploadDirectory,
                strategy: .platformDefault,
                recursively: true
            )
        }
        catch let error as StorageClientError {
            throw error
        }
        catch {
            throw .unknown(error)
        }
    }

    // MARK: - helpers

    private func resolvePath(
        for key: String
    ) throws(StorageClientError) -> FilePath {
        let components = key.split(
            separator: "/",
            omittingEmptySubsequences: true
        )
        for component in components where component == "." || component == ".."
        {
            throw .invalidKey
        }

        if components.isEmpty {
            return .init(rootPath)
        }

        let relative = components.joined(separator: "/")
        return .init(rootPath + "/" + relative)
    }

    private func parentPath(
        of path: FilePath
    ) -> FilePath {
        let raw = path.description
        if raw.isEmpty || raw == "/" {
            return .init(raw)
        }
        if let index = raw.lastIndex(of: "/") {
            if index == raw.startIndex {
                return .init("/")
            }
            return .init(String(raw[..<index]))
        }
        return .init(".")
    }

    private func multipartDirectory(
        for key: String,
        uploadId: String
    ) throws(StorageClientError) -> FilePath {
        try resolvePath(for: ".multipart/\(key)/\(uploadId)")
    }

    private func randomIdentifier(prefix: String) -> String {
        "\(prefix)-\(UInt64.random(in: .min ... .max))"
    }

    private static func trimTrailingSlashes(_ value: String) -> String {
        var value = value
        while value.count > 1, value.last == "/" {
            value.removeLast()
        }
        return value
    }
}
