#[path = "goodbye.rs"]
mod goodbye;
#[path = "hello.rs"]
mod hello;

pub use goodbye::Goodbye;
pub use hello::Hello;
