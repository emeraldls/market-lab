FORGE ?= forge
SOLC ?= 0.8.35
RPC ?= http://127.0.0.1:8545

.PHONY: deps build check demo format

deps:
	$(FORGE) install --root . --no-git OpenZeppelin/openzeppelin-contracts@rev=5fd1781b1454fd1ef8e722282f86f9293cacf256
	$(FORGE) install --root . --no-git foundry-rs/forge-std@rev=bf647bd6046f2f7da30d0c2bf435e5c76a780c1b

build:
	$(FORGE) build --use $(SOLC) -j 1

check:
	$(FORGE) script script/Smoke.s.sol:Smoke --use $(SOLC) --chain 31337 -j 1

demo:
	$(FORGE) script script/Smoke.s.sol:Smoke --use $(SOLC) --rpc-url $(RPC) --broadcast --slow -j 1

format:
	$(FORGE) fmt
