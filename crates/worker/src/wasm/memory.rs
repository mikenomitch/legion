//! Guest memory, from inside a host function. Pointers and lengths arrive
//! as `i32` and are read as the `u32` the WASM ABI means, so a length of
//! `usize::MAX` on the guest side (-1) is a bounds error, not a negative.

use wasmi::{Caller, Extern};

/// The module's exported `memory`.
pub fn memory<T>(caller: &Caller<'_, T>) -> Result<wasmi::Memory, wasmi::Error> {
    caller
        .get_export("memory")
        .and_then(Extern::into_memory)
        .ok_or_else(|| wasmi::Error::new("the module exports no memory"))
}

/// Copy `len` bytes out of guest memory at `ptr`.
pub fn read<T>(caller: &Caller<'_, T>, ptr: i32, len: i32) -> Result<Vec<u8>, wasmi::Error> {
    let (ptr, len) = (ptr as u32 as usize, len as u32 as usize);
    let memory = memory(caller)?;
    if ptr.saturating_add(len) > memory.data_size(caller) {
        return Err(wasmi::Error::new("read outside the module's memory"));
    }
    let mut out = vec![0u8; len];
    memory
        .read(caller, ptr, &mut out)
        .map_err(|_| wasmi::Error::new("read outside the module's memory"))?;
    Ok(out)
}

/// [`read`], as UTF-8 (lossily).
pub fn read_string<T>(caller: &Caller<'_, T>, ptr: i32, len: i32) -> Result<String, wasmi::Error> {
    Ok(String::from_utf8_lossy(&read(caller, ptr, len)?).into_owned())
}

/// Copy `data` into guest memory at `ptr`.
pub fn write<T>(caller: &mut Caller<'_, T>, ptr: i32, data: &[u8]) -> Result<(), wasmi::Error> {
    let memory = memory(caller)?;
    memory
        .write(caller, ptr as u32 as usize, data)
        .map_err(|_| wasmi::Error::new("write outside the module's memory"))
}

/// Write one little-endian `u32` (a length, a pointer, a count).
pub fn write_u32<T>(caller: &mut Caller<'_, T>, ptr: i32, value: u32) -> Result<(), wasmi::Error> {
    write(caller, ptr, &value.to_le_bytes())
}
