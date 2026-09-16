//
//  FileStorageSequenceTestSuite.swift
//  feather-storage-fs
//
//  Created by Tibor Bödecs on 2023. 01. 16.

import FeatherStorage
import NIOCore
import Testing
import _NIOFileSystem

@testable import FeatherStorageFS

@Suite
struct FileStorageSequenceTestSuite {

    @Test
    func closesHandleAfterReachingEnd() async throws {
        let fileSystem = FileSystem.shared
        let rootPath =
            "/tmp/feather-storage-fs-tests-\(UInt64.random(in: .min ... .max))"
        try await fileSystem.createDirectory(
            at: .init(rootPath),
            withIntermediateDirectories: true
        )
        defer {
            Task {
                _ = try? await fileSystem.removeItem(
                    at: .init(rootPath),
                    strategy: .platformDefault,
                    recursively: true
                )
            }
        }

        let storage = StorageClientFS(rootPath: rootPath)
        var data = ByteBufferAllocator().buffer(capacity: 10)
        data.writeString("0123456789")
        try await storage.upload(
            key: "value.txt",
            sequence: .init(buffer: data)
        )

        let handle = try await fileSystem.openFile(
            forReadingAt: .init(rootPath + "/value.txt")
        )
        let sequence = FileStorageSequence(
            handle: handle,
            chunks: handle.readChunks(in: 0..<10, chunkLength: .bytes(4))
        )
        var iterator = sequence.makeAsyncIterator()
        while try await iterator.next() != nil {}

        do {
            _ = try await handle.info()
            Issue.record("Expected the file handle to be closed")
        }
        catch let error as FileSystemError {
            #expect(error.code == .closed)
        }
        catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test
    func iteratorOutlivesSequence() async throws {
        let fileSystem = FileSystem.shared
        let rootPath =
            "/tmp/feather-storage-fs-tests-\(UInt64.random(in: .min ... .max))"
        try await fileSystem.createDirectory(
            at: .init(rootPath),
            withIntermediateDirectories: true
        )
        defer {
            Task {
                _ = try? await fileSystem.removeItem(
                    at: .init(rootPath),
                    strategy: .platformDefault,
                    recursively: true
                )
            }
        }

        let storage = StorageClientFS(rootPath: rootPath)
        var data = ByteBufferAllocator().buffer(capacity: 10)
        data.writeString("0123456789")
        try await storage.upload(
            key: "value.txt",
            sequence: .init(buffer: data)
        )

        let handle = try await fileSystem.openFile(
            forReadingAt: .init(rootPath + "/value.txt")
        )
        var iterator = FileStorageSequence(
            handle: handle,
            chunks: handle.readChunks(in: 0..<10, chunkLength: .bytes(4))
        ).makeAsyncIterator()

        for _ in 0..<10 {
            await Task.yield()
        }

        var result = ByteBufferAllocator().buffer(capacity: 10)
        while let chunk = try await iterator.next() {
            var chunk = chunk
            result.writeBuffer(&chunk)
        }
        #expect(
            result.getString(at: result.readerIndex, length: result.readableBytes)
                == "0123456789"
        )
    }

    @Test
    func closesHandleWhenIterationIsAbandoned() async throws {
        let fileSystem = FileSystem.shared
        let rootPath =
            "/tmp/feather-storage-fs-tests-\(UInt64.random(in: .min ... .max))"
        try await fileSystem.createDirectory(
            at: .init(rootPath),
            withIntermediateDirectories: true
        )
        defer {
            Task {
                _ = try? await fileSystem.removeItem(
                    at: .init(rootPath),
                    strategy: .platformDefault,
                    recursively: true
                )
            }
        }

        let storage = StorageClientFS(rootPath: rootPath)
        var data = ByteBufferAllocator().buffer(capacity: 10)
        data.writeString("0123456789")
        try await storage.upload(
            key: "value.txt",
            sequence: .init(buffer: data)
        )

        let handle = try await fileSystem.openFile(
            forReadingAt: .init(rootPath + "/value.txt")
        )
        do {
            let sequence = FileStorageSequence(
                handle: handle,
                chunks: handle.readChunks(in: 0..<10, chunkLength: .bytes(4))
            )
            var iterator = sequence.makeAsyncIterator()
            _ = try await iterator.next()
        }

        var isClosed = false
        for _ in 0..<100 {
            do {
                _ = try await handle.info()
                try await Task.sleep(for: .milliseconds(1))
            }
            catch let error as FileSystemError where error.code == .closed {
                isClosed = true
                break
            }
            catch {
                Issue.record("Unexpected error: \(error)")
                break
            }
        }
        #expect(isClosed)
    }
}
