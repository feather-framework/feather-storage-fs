//
//  LocalStorageComponent.swift
//  FeatherStorageDriverLocal
//
//  Created by Tibor Bödecs on 2020. 04. 28..
//

import FeatherComponent
import FeatherStorage
import Foundation
import NIO
import NIOFoundationCompat
import _NIOFileSystem

@dynamicMemberLookup
struct LocalStorageComponent {

    let config: ComponentConfig

    subscript<T>(
        dynamicMember keyPath: KeyPath<LocalStorageComponentContext, T>
    ) -> T {
        let context = config.context as! LocalStorageComponentContext
        return context[keyPath: keyPath]
    }
}

extension LocalStorageComponent {

    fileprivate func url(for key: String?) -> URL {
        .init(
            fileURLWithPath: self.path
        )
        .appendingPathComponent(key ?? "")
    }
}

class FileChunksAsyncSequenceWrapper: AsyncSequence {
    typealias Element = ByteBuffer

    let handler: ReadFileHandle
    let fileChunks: FileChunks
    let length: UInt64

    init(path: String, range: ClosedRange<Int>?) async throws {
        handler = try await FileSystem.shared.openFile(
            forReadingAt: .init(path)
        )

        let size = try await handler.info().size

        if let range, range.lowerBound >= 0, range.upperBound < size {
            fileChunks = handler.readChunks(
                in: Int64(range.lowerBound)...Int64(range.upperBound),
                chunkLength: .kilobytes(32)
            )
            length = UInt64(range.upperBound - range.lowerBound + 1)
        }
        else {
            fileChunks = handler.readChunks(
                in: 0..<size,
                chunkLength: .kilobytes(32)
            )
            length = UInt64(size)
        }
    }

    struct AsyncIterator: AsyncIteratorProtocol {
        var iterator: FileChunks.FileChunkIterator

        mutating func next() async throws -> ByteBuffer? {
            try await iterator.next()
        }
    }

    func makeAsyncIterator() -> AsyncIterator {
        .init(iterator: fileChunks.makeAsyncIterator())
    }

    deinit {
        let semaphore = DispatchSemaphore(value: 0)
        Task {
            do {
                try await handler.close()
            }
            catch {
                //catch all
            }
            semaphore.signal()
        }
        semaphore.wait()
    }
}

extension LocalStorageComponent: StorageComponent {

    public var availableSpace: UInt64 {
        let attributes = try? FileManager.default.attributesOfFileSystem(
            forPath: NSHomeDirectory() as String
        )
        let freeSpace = (attributes?[.systemFreeSize] as? NSNumber)?.int64Value
        return UInt64(freeSpace ?? 0)
    }

    public func uploadStream(
        key: String,
        sequence: StorageAnyAsyncSequence<ByteBuffer>
    ) async throws {
        let fileUrl = url(for: key)
        let location = fileUrl.deletingLastPathComponent()
        try FileManager.default.createDirectory(
            at: location,
            permission: self.posixMode
        )

        let fileio = NonBlockingFileIO(threadPool: self.threadPool)
        let handle = try await fileio.openFile(
            path: fileUrl.path,
            mode: .write,
            flags: .allowFileCreation(),
            eventLoop: self.eventLoopGroup.next()
        )
        do {
            var iterator = sequence.makeAsyncIterator()

            while let byteBuffer = try await iterator.next() {
                try await fileio.write(
                    fileHandle: handle,
                    buffer: byteBuffer,
                    eventLoop: self.eventLoopGroup.next()
                )
            }

            try handle.close()
        }
        catch {
            try handle.close()
            throw error
        }
    }

    func downloadStream(key: String, range: ClosedRange<Int>?) async throws
        -> StorageAnyAsyncSequence<ByteBuffer>
    {
        let exists = await exists(key: key)
        guard exists else {
            throw StorageComponentError.invalidKey
        }
        let sourceUrl = url(for: key)
        let sequence = try await FileChunksAsyncSequenceWrapper(
            path: sourceUrl.path,
            range: range
        )

        return .init(asyncSequence: sequence, length: sequence.length)
    }

    public func exists(key: String) async -> Bool {
        FileManager.default.fileExists(atPath: url(for: key).path)
    }

    public func size(key: String) async -> UInt64 {
        let exists = await exists(key: key)
        guard exists else {
            return 0
        }
        let sourceUrl = url(for: key)
        let fileio = NonBlockingFileIO(threadPool: self.threadPool)
        guard
            let handle = try? await fileio.openFile(
                path: sourceUrl.path,
                mode: .read,
                eventLoop: self.eventLoopGroup.next()
            )
        else {
            return 0
        }
        do {
            let size = try await fileio.readFileSize(
                fileHandle: handle,
                eventLoop: self.eventLoopGroup.next()
            )
            try? handle.close()
            return .init(size)
        }
        catch {
            try? handle.close()
            return 0
        }
    }

    public func copy(
        key source: String,
        to destination: String
    ) async throws {
        let exists = await exists(key: source)
        guard exists else {
            throw StorageComponentError.invalidKey
        }
        try await delete(key: destination)
        let sourceUrl = url(for: source)
        let destinationUrl = url(for: destination)
        let location = destinationUrl.deletingLastPathComponent()
        try FileManager.default.createDirectory(
            at: location,
            permission: self.posixMode
        )
        try FileManager.default.copyItem(at: sourceUrl, to: destinationUrl)
    }

    public func list(key: String?) async throws -> [String] {
        let dirUrl = url(for: key ?? "")
        if FileManager.default.directoryExists(at: dirUrl) {
            return try FileManager.default.contentsOfDirectory(
                atPath: dirUrl.path
            )
        }
        return []
    }

    public func delete(key: String) async throws {
        guard await exists(key: key) else {
            return
        }
        try FileManager.default.removeItem(atPath: url(for: key).path)
    }

    public func create(key: String) async throws {
        try FileManager.default.createDirectory(
            at: url(for: key),
            withIntermediateDirectories: true
        )
    }

    public func createMultipartId(
        key: String
    ) async throws -> String {
        let multipartId = UUID().uuidString
        let multipartKey = "multipart/\(key)/\(multipartId)"
        try await create(key: multipartKey)
        return multipartId
    }

    func uploadStream(
        multipartId: String,
        key: String,
        number: Int,
        sequence: StorageAnyAsyncSequence<ByteBuffer>
    ) async throws -> StorageChunk {
        let chunkId = UUID().uuidString
        let multipartKey = "multipart/\(key)/\(multipartId)"
        let chunkKey = "\(multipartKey)/\(chunkId)-\(number)"
        try await uploadStream(key: chunkKey, sequence: sequence)
        return .init(chunkId: chunkId, number: number)
    }

    public func abort(
        multipartId: String,
        key: String
    ) async throws {
        let multipartKey = "multipart/\(key)/\(multipartId)"
        try await delete(key: multipartKey)
    }

    public func finish(
        multipartId: String,
        key: String,
        chunks: [StorageChunk]
    ) async throws {
        let multipartKey = "multipart/\(key)/\(multipartId)"
        let fileUrl = url(for: key)
        FileManager.default.createFile(
            atPath: fileUrl.path,
            contents: nil
        )
        guard let writeHandle = FileHandle(forWritingAtPath: fileUrl.path)
        else {
            throw StorageComponentError.invalidKey
        }

        for chunk in chunks.sorted(by: { $0.number < $1.number }) {
            let chunkKey = "\(multipartKey)/\(chunk.chunkId)-\(chunk.number)"
            let chunkUrl = url(for: chunkKey)

            guard let readHandle = FileHandle(forReadingAtPath: chunkUrl.path)
            else {
                throw StorageComponentError.invalidKey
            }
            let chunkSize = try FileManager.default.size(at: chunkUrl)
            let data = readHandle.readData(ofLength: Int(chunkSize))
            writeHandle.write(data)
            try readHandle.close()
        }
        try writeHandle.close()
        try await delete(key: multipartKey)
    }
}
