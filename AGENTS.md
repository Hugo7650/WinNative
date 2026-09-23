# Repository Guidelines

WinNative is a multi-module Android application combining Kotlin/Java UI and services with C/C++ native emulation and graphics code. Keep changes focused and preserve upstream license headers and attribution files.

## Project Structure & Module Organization

- `app/src/main/app`, `feature`, `runtime`, `shared`, and `engine` contain the Android application code; resources and packaged runtime data live in `app/src/main/res` and `app/src/main/assets`.
- Native code and CMake configuration are under `app/src/main/cpp`. The `adrenotools` and `vkbasalt` dependencies are recursive Git submodules.
- Gradle modules are `:app`, `:framegen`, `:armsx2`, and `:dolphin`; `docs/` contains build and feature notes, while `tools/` holds pinned dependencies and maintenance scripts.
- JVM tests are in `app/src/test/kotlin`; device/UI tests are in `app/src/androidTest/kotlin`.

## Build, Test, and Development Commands

Use JDK 17, Android SDK 35, NDK `27.3.13750724`, and CMake 3.22.1. After cloning, run `git lfs pull` and `git submodule update --init --recursive`.

- `./gradlew assembleStandardDebug` — build the standard debug APK.
- `./gradlew assemblePubgDebug` — build a branded flavor; substitute `Ludashi` or `Antutu` as needed. On Windows, use `./gradlew.bat`.
- `./gradlew :app:testStandardDebugUnitTest` — run JVM tests with JUnit, Robolectric, and Mockito.
- `./gradlew :app:connectedStandardDebugAndroidTest` — run instrumentation and Compose UI tests on a connected device/emulator.
- `./gradlew :app:spotlessApply` / `./gradlew :app:spotlessCheck` — format or validate Java/Kotlin formatting.

## Coding Style & Naming Conventions

Use four-space indentation, lowercase package names, `UpperCamelCase` types, and `lowerCamelCase` members. Name tests after the subject, for example `DeviceProfileTest.kt`. Kotlin is checked with ktlint and Java with Google Java Format through Spotless; follow nearby conventions in native C/C++ and GLSL files.

## Testing Guidelines

Add a focused regression test for behavior changes. Keep pure logic in `src/test`; use `src/androidTest` when Android framework, Compose, JNI, or hardware behavior is required. There is no documented coverage threshold, but all relevant unit tests and affected flavor builds should pass before opening a PR.

## Commit & Pull Request Guidelines

Use a short imperative subject, optionally prefixed with `Fix:`, `Feature:`, or `Update:`; include an issue/PR reference such as `(#123)` when applicable. PRs should explain the user-visible impact, list validation commands, identify affected flavors/modules, and include screenshots or device/Android-version details for UI or platform changes. Do not commit signing keys or local configuration; use environment variables or ignored `signing.properties` for release credentials.
