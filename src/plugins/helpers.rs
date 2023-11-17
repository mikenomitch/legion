// /// Declare a plugin type and its constructor.
// ///
// /// # Notes
// ///
// /// This works by automatically generating an `extern "C"` function with a
// /// pre-defined signature and symbol name. Therefore you will only be able to
// /// declare one plugin per library.
// #[macro_export]
// macro_rules! declare_plugin {
//     ($plugin_type:ty, $constructor:path) => {
//         #[no_mangle]
//         pub extern "C" fn _plugin_create() -> *mut $crate::Plugin {
//             // make sure the constructor is the correct type.
//             let constructor: fn() -> $plugin_type = $constructor;

//             let object = constructor();
//             let boxed: Box<$crate::Plugin> = Box::new(object);
//             Box::into_raw(boxed)
//         }
//     };
// }

// pub struct PluginManager {
//     plugins: Vec<Box<Plugin>>,
//     loaded_libraries: Vec<Library>,
// }

// impl PluginManager {
//     pub fn new() -> PluginManager {
//         PluginManager {
//             plugins: Vec::new(),
//             loaded_libraries: Vec::new(),
//         }
//     }
// }


// pub unsafe fn load_plugin<P: AsRef<OsStr>>(&mut self, filename: P) -> Result<()> {
//     type PluginCreate = unsafe fn() -> *mut Plugin;

//     let lib = Library::new(filename.as_ref()).chain_err(|| "Unable to load the plugin")?;

//     // We need to keep the library around otherwise our plugin's vtable will
//     // point to garbage. We do this little dance to make sure the library
//     // doesn't end up getting moved.
//     self.loaded_libraries.push(lib);

//     let lib = self.loaded_libraries.last().unwrap();

//     let constructor: Symbol<PluginCreate> = lib.get(b"_plugin_create")
//         .chain_err(|| "The `_plugin_create` symbol wasn't found.")?;
//     let boxed_raw = constructor();

//     let plugin = Box::from_raw(boxed_raw);
//     debug!("Loaded plugin: {}", plugin.name());
//     plugin.on_plugin_load();
//     self.plugins.push(plugin);


//     Ok(())
}
