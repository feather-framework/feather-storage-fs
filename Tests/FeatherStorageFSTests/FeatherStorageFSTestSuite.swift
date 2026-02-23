//
//  FeatherStorageFSTestSuite.swift
//  feather-storage-fs
//
//  Created by Tibor Bödecs on 2023. 01. 16.

import FeatherStorage
import NIOCore
import Testing
import _NIOFileSystem

@testable import FeatherStorageFS

@Suite
struct FeatherStorageFSTestSuite {

    @Test
    func uploadDownloadAndList() async throws {
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

        var data = ByteBufferAllocator().buffer(capacity: 0)
        data.writeString("filesystem-driver")

        let seq = ByteBufferSequence(buffer: data)
        try await storage.upload(
            key: "images/logo.txt",
            sequence: .init(asyncSequence: seq)
        )

        let keys = try await storage.list(key: "images")
        #expect(keys == ["logo.txt"])

        let downloaded = try await storage.download(
            key: "images/logo.txt",
            range: nil
        )
        let res = try await downloaded.collect(upTo: .max)
        #expect(
            res.getString(at: res.readerIndex, length: res.readableBytes)
                == "filesystem-driver"
        )
    }

    @Test
    func multipartUpload() async throws {
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
        let uploadId = try await storage.createMultipartId(
            key: "videos/final.txt"
        )

        var first = ByteBufferAllocator().buffer(capacity: 0)
        first.writeString("part-")
        var second = ByteBufferAllocator().buffer(capacity: 0)
        second.writeString("two")

        let firstSeq = ByteBufferSequence(buffer: first)
        let secondSeq = ByteBufferSequence(buffer: second)

        let p1 = try await storage.upload(
            multipartId: uploadId,
            key: "videos/final.txt",
            number: 1,
            sequence: .init(asyncSequence: firstSeq)
        )
        let p2 = try await storage.upload(
            multipartId: uploadId,
            key: "videos/final.txt",
            number: 2,
            sequence: .init(asyncSequence: secondSeq)
        )
        try await storage.finish(
            multipartId: uploadId,
            key: "videos/final.txt",
            chunks: [p1, p2]
        )

        let merged = try await storage.download(
            key: "videos/final.txt",
            range: nil
        )
        let m = try await merged.collect(upTo: .max)

        #expect(
            m.getString(at: m.readerIndex, length: m.readableBytes)
                == "part-two"
        )
    }
}
