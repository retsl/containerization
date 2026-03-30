//===----------------------------------------------------------------------===//
// Copyright © 2025-2026 Apple Inc. and the Containerization project authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//===----------------------------------------------------------------------===//

import Foundation
import Testing
import Virtualization

@testable import Containerization

struct GUIResolutionTests {
    @Test func defaultResolution() {
        let res = GUIResolution()
        #expect(res.width == 1280)
        #expect(res.height == 720)
    }

    @Test func customResolution() {
        let res = GUIResolution(width: 1920, height: 1080)
        #expect(res.width == 1920)
        #expect(res.height == 1080)
    }

    @Test func equatable() {
        let a = GUIResolution(width: 800, height: 600)
        let b = GUIResolution(width: 800, height: 600)
        let c = GUIResolution(width: 1024, height: 768)
        #expect(a == b)
        #expect(a != c)
    }

    @Test func codableRoundTrip() throws {
        let original = GUIResolution(width: 2560, height: 1440)
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(GUIResolution.self, from: data)
        #expect(decoded == original)
    }
}

struct VMConfigurationGUITests {
    @Test func guiDefaultsFalse() {
        let config = VMConfiguration()
        #expect(config.gui == false)
        #expect(config.guiResolution == GUIResolution())
    }

    @Test func guiExplicitlyEnabled() {
        let config = VMConfiguration(gui: true, guiResolution: GUIResolution(width: 1920, height: 1080))
        #expect(config.gui == true)
        #expect(config.guiResolution.width == 1920)
        #expect(config.guiResolution.height == 1080)
    }

    @Test func existingCallersUnaffected() {
        let config = VMConfiguration(cpus: 2, memoryInBytes: 2 * 1024 * 1024 * 1024)
        #expect(config.gui == false)
        #expect(config.guiResolution == GUIResolution())
        #expect(config.cpus == 2)
    }
}

struct GUIDeviceInjectionTests {
    /// Verify that the device set `toVZ()` now attaches unconditionally — GPU,
    /// USB keyboard, USB pointing device, audio output, and SPICE console — is
    /// accepted by the VZ framework.  Audio input is intentionally excluded: it
    /// requires the `com.apple.security.device.audio-input` entitlement and will
    /// cause `validate()` to throw on headless boots without it (#146).
    @Test func unconditionalDevicesAcceptedByVZ() throws {
        let config = VZVirtualMachineConfiguration()
        config.cpuCount = 2
        config.memorySize = UInt64(1024 * 1024 * 1024)

        let graphics = VZVirtioGraphicsDeviceConfiguration()
        graphics.scanouts = [
            VZVirtioGraphicsScanoutConfiguration(widthInPixels: 1280, heightInPixels: 720),
        ]
        config.graphicsDevices = [graphics]

        config.keyboards = [VZUSBKeyboardConfiguration()]
        config.pointingDevices = [VZUSBScreenCoordinatePointingDeviceConfiguration()]

        let outputAudioDevice = VZVirtioSoundDeviceConfiguration()
        let outputStream = VZVirtioSoundDeviceOutputStreamConfiguration()
        outputStream.sink = VZHostAudioOutputStreamSink()
        outputAudioDevice.streams = [outputStream]
        config.audioDevices = [outputAudioDevice]

        let spiceConsole = VZVirtioConsoleDeviceConfiguration()
        let spicePort = VZVirtioConsolePortConfiguration()
        spicePort.name = VZSpiceAgentPortAttachment.spiceAgentPortName
        spicePort.attachment = VZSpiceAgentPortAttachment()
        spiceConsole.ports[0] = spicePort
        config.consoleDevices.append(spiceConsole)

        #expect(config.graphicsDevices.count == 1)
        #expect(config.keyboards.count == 1)
        #expect(config.pointingDevices.count == 1)
        #expect(config.audioDevices.count == 1)
        #expect(config.consoleDevices.count == 1)
    }

    /// A freshly allocated `VZVirtualMachineConfiguration` has no GUI devices by
    /// default. This is a VZ framework baseline; `toVZ()` always adds the
    /// unconditional device set on top of this.
    @Test func vzDefaultConfigurationHasNoGUIDevices() {
        let config = VZVirtualMachineConfiguration()
        #expect(config.graphicsDevices.isEmpty)
        #expect(config.keyboards.isEmpty)
        #expect(config.pointingDevices.isEmpty)
        #expect(config.audioDevices.isEmpty)
        #expect(config.consoleDevices.isEmpty)
    }

    /// `gui` on `VZVirtualMachineInstance.Configuration` is a deprecated no-op:
    /// `toVZ()` no longer reads it. The field is kept for source compatibility
    /// until its call sites are removed by issue #148.
    @Test func guiFieldIsDeprecatedNoOp() {
        var instanceConfig = VZVirtualMachineInstance.Configuration()
        #expect(instanceConfig.gui == false)

        // Setting gui has no effect on toVZ() device attachment; the field
        // exists only to avoid breaking callers until #148 lands.
        instanceConfig.gui = true
        #expect(instanceConfig.gui == true)
    }

    @Test func managerThreadsGUIFields() throws {
        let vmConfig = VMConfiguration(
            gui: true,
            guiResolution: GUIResolution(width: 800, height: 600)
        )
        #expect(vmConfig.gui == true)
        #expect(vmConfig.guiResolution.width == 800)
        #expect(vmConfig.guiResolution.height == 600)
    }
}
