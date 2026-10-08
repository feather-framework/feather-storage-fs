// swift-tools-version:6.3
import PackageDescription

let swiftSettings: [SwiftSetting] = [
    .swiftLanguageMode(.v6),
    .strictMemorySafety(),
    .treatAllWarnings(as: .error),
    .enableUpcomingFeature("InternalImportsByDefault"),
    .enableUpcomingFeature("ExistentialAny"),
    .enableUpcomingFeature("MemberImportVisibility"),
    .enableUpcomingFeature("InferIsolatedConformances"),
    .enableUpcomingFeature("NonisolatedNonsendingByDefault"),
    .enableUpcomingFeature("ImmutableWeakCaptures"),
    .enableExperimentalFeature("SuppressedAssociatedTypes"),
    .enableExperimentalFeature("LifetimeDependence"),
    .enableExperimentalFeature("Lifetimes"),
    .enableUpcomingFeature("StrictConcurrency"),
]

let package = Package(
    name: "feather-storage-fs",
    platforms: [
        .macOS(.v15),
        .iOS(.v18),
        .tvOS(.v18),
        .watchOS(.v11),
        .visionOS(.v2),
    ],
    products: [
        .library(name: "FeatherStorageFS", targets: ["FeatherStorageFS"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio", from: "2.100.0"),
        .package(path: "../feather-storage"),
    ],
    targets: [
        .target(
            name: "FeatherStorageFS",
            dependencies: [
                .product(name: "_NIOFileSystem", package: "swift-nio"),
                .product(name: "FeatherStorage", package: "feather-storage"),
                
            ],
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "FeatherStorageFSTests",
            dependencies: [
                .target(name: "FeatherStorageFS"),
                .product(name: "FeatherStorage", package: "feather-storage"),
            ],
            swiftSettings: swiftSettings
        ),
    ]
)
