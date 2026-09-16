//
//  FileStorageAsyncSequence.swift
//  feather-storage-fs
//
//  Created by Tibor Bödecs on 2023. 01. 16.

import NIOCore
import Synchronization
import _NIOFileSystem

/// A pull-based sequence of chunks from an open file handle.
struct FileStorageAsyncSequence: AsyncSequence, Sendable {
    typealias Element = ByteBuffer

    fileprivate final class HandleState: Sendable {
        private struct State {
            var hasIterator = false
            var isClosing = false
        }

        let handle: ReadFileHandle
        private let state = Mutex(State())

        init(
            handle: ReadFileHandle
        ) {
            self.handle = handle
        }

        func registerIterator() {
            state.withLock { state in
                state.hasIterator = true
            }
        }

        func sequenceReleased() {
            let shouldClose = state.withLock { state in
                guard !state.hasIterator, !state.isClosing else {
                    return false
                }
                state.isClosing = true
                return true
            }
            closeInBackgroundIfNeeded(shouldClose)
        }

        func iteratorReleased() {
            let shouldClose = state.withLock { state in
                guard !state.isClosing else {
                    return false
                }
                state.isClosing = true
                return true
            }
            closeInBackgroundIfNeeded(shouldClose)
        }

        func close() async throws {
            let shouldClose = state.withLock { state in
                guard !state.isClosing else {
                    return false
                }
                state.isClosing = true
                return true
            }
            if shouldClose {
                try await handle.close()
            }
        }

        private func closeInBackgroundIfNeeded(
            _ shouldClose: Bool
        ) {
            guard shouldClose else {
                return
            }
            let handle = handle
            Task {
                try? await handle.close()
            }
        }
    }

    fileprivate final class SequenceLifetime: Sendable {
        let state: HandleState

        init(
            state: HandleState
        ) {
            self.state = state
        }

        deinit {
            state.sequenceReleased()
        }
    }

    fileprivate final class IteratorLifetime: Sendable {
        let state: HandleState

        init(
            state: HandleState
        ) {
            self.state = state
        }

        deinit {
            state.iteratorReleased()
        }
    }

    struct AsyncIterator: AsyncIteratorProtocol {
        private var base: FileChunks.FileChunkIterator
        private let lifetime: IteratorLifetime

        fileprivate init(
            base: FileChunks.FileChunkIterator,
            lifetime: IteratorLifetime
        ) {
            self.base = base
            self.lifetime = lifetime
        }

        mutating func next(
            isolation actor: isolated (any Actor)?
        ) async throws -> ByteBuffer? {
            do {
                let chunk = try await base.next(isolation: actor)
                if chunk == nil {
                    try await lifetime.state.close()
                }
                return chunk
            }
            catch {
                try? await lifetime.state.close()
                throw error
            }
        }
    }

    private let chunks: FileChunks
    private let lifetime: SequenceLifetime

    init(
        handle: ReadFileHandle,
        chunks: FileChunks
    ) {
        self.chunks = chunks
        self.lifetime = .init(state: .init(handle: handle))
    }

    func makeAsyncIterator() -> AsyncIterator {
        lifetime.state.registerIterator()
        return .init(
            base: chunks.makeAsyncIterator(),
            lifetime: .init(state: lifetime.state)
        )
    }
}
