# Repository State

1. Documentation is added before a new feature is implemented:
    - Search the [/doc](/doc) directory for markdown files relevant to your task.
    - Add a markdown file to the [/doc](/doc) directory before implementing any substantial new feature.
    - Update existing documentation if you alter the behaviour of an existing feature.
2. Committed code is authoritative; only alter if absolutely necessary. Explain why the change is necessary.
3. Uncommitted code represents work-in-progress and can be readily updated.

# Style Guidance

1. Your contributions must closely follow the neat style of committed code.
2. Ignore the style of uncommitted code as this may not be representative of the final style.
3. Prefer smaller composable modular functions following the existing committed code style.
4. Use single-word names when possible.
5. Include comprehensive documentation using the exact style of existing committed doc comments.
6. Documentation for functions that return `Result` should describe the possible error reasons.
    - Trait function documentation should not include error reasons as these may be implementation-specific.
    - Instead, each trait impl should override the trait function documentation to include the specific error reasons
      if applicable, and link to the trait function documentation for "more information".
7. Prefer generic functions with trait bounds to improve future flexibility.
8. Prefer type-driven compile-time dispatch such as `my_function::<MyType>()` to minimise runtime overhead.
9. Do not generate new declarative macros as these obfuscate the true code. Prefer concrete implementations.
10. Using existing external macros is allowed e.g. `write!`
11. Prefer to use built-in traits instead of defining a new trait where possible.
12. Never use `<(..)>` syntax e.g. `Vec` wrapping a tuple.
13. Never use `Result<(), ..>` or `Option<()>` syntax.
14. Use the `?` operator for error handling where possible to improve readability and reduce boilerplate.
15. Avoid cloning data. Use references and borrowing where possible to reduce allocations.
16. The `bitvec` crate includes many optimised functions for working with single bits. Use the `bitvec` built-in
    functions. Do not implement manual bitwise operations.
17. Prefer vertically symmetrical letters for type generics e.g. `I` instead of `T` or `E` instead of `F`.
    - Type generics should use the first letter of their semantic meaning e.g. `I` for `Item` or `E` for `Error`.
18. Prefer associated functions to free functions.
19. Add all supported `#[derive]` attributes to extend functionality and reduce boilerplate.

# Testing and Verification

1. Add small unit tests at the bottom of each module.
2. Add round-trip tests in the test directory.
3. Verify with `cargo build` and `cargo test` unless instructed otherwise.

# General Guidance

1. Ask questions if anything is uncertain. Do not attempt to guess the user’s intention if there is ambiguity.
2. Use `cargo +nightly fmt` to format touched files only; never reformat files that are not touched during your edit.
3. A project TODO list is maintained in [todo.md](../todo.md):
    - Read [todo.md](../todo.md) before starting work to understand how your task fits into the project roadmap.
    - Mark tasks as complete in [todo.md](../todo.md) when finished; add bullet points w/ relevant details.
    - Unmark completed tasks if your edit removes previously implemented functionality.
    - The [todo.md](../todo.md) file must always accurately reflect the current state of the project.