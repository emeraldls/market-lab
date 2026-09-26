# Managed Runtime Follow-Ups

- [ ] Browser-wallet agent authorization: expose a Rust flow to create agent credentials, return the public key and approval payload for the user's browser wallet to sign, and complete authorization. Keep the main wallet private key out of the backend. Store the agent private key only in that user's runtime credential store.
- [ ] Shared market metadata: let independent runtimes read one shared market-snapshot directory instead of copying catalogs into every `MLAB_HOME`. Give containers read-only access; refresh snapshots centrally and support reloading them safely. Keep credentials, account data, orders and jobs private to each runtime. This is about market catalogs and trading rules; sharing live feeds is a separate design decision.
