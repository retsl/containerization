#if os(macOS)
import Virtualization

/// A `VirtualMachineInstance` that can expose its underlying `VZVirtualMachine`.
///
/// Conform to this protocol to allow code outside the `Containerization` module
/// (e.g. `DuxVMWindow`) to attach a `VZVirtualMachineView` to the VM without
/// a direct dependency on the internal `VZVirtualMachineInstance` type.
public protocol VZVirtualMachineProviding: VirtualMachineInstance, Sendable {
    /// The underlying `VZVirtualMachine` object.
    var underlying: VZVirtualMachine { get }
}
#endif
