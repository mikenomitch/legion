# Next Up

- Figure out what to do about ids for job and job_config and whatnot

# Other shovel ready work

- Nicer errors in CLI commands for various failures
  - Nice storage errors generally
    - I think I'll have to have different error types
      for the storage layer that the sql errors translate into
      and then the layer calling the store has to deal with these
      at a slightly different layer of abstraction
    - See this page for in-store error handling (https://users.rust-lang.org/t/handle-rusqlite-error/39660/7)

- Think about your file setup (names/org/dupes)
  - Where do structs get their definition

- Add more tracing - especially to the scheduler and eval loop

- Worker configs CRUD commands with push from manager
  - I guess for this to really work, you would need to update any of this type
    that already exist (and send them the info)
  - Which means that nodes should save a reference to their type

- Fix the join-token and pre-register call destinations
  - Should the "insecure client" really just be a "public cert client"
    - Gets from lets encrypt by default but you can provide your own CA too?
  - Then the "client" is the "secure client" which does some self-signed CA magic
    and only the workers have access to it?
  - Or is TLS just not right right way to do this at all?
  - !! You need to plan this and not just code it!!

  - Environment variable or flag used for contacting manager
    - How to nicely join-server port from this?
      - You don't need it because its in the JWT!
