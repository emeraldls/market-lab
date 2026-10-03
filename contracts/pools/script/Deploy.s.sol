// SPDX-License-Identifier: GPL-3.0-only
pragma solidity ^0.8.24;

import {Script, console2} from "forge-std/Script.sol";
import {PoolFactory} from "../src/PoolFactory.sol";
import {WrappedHype} from "../src/WrappedHype.sol";
import {DemoToken} from "./DemoToken.sol";

/// @notice Deploy with a Foundry keystore (--account), not a private key in source or arguments.
contract Deploy is Script {
    function run() external {
        require(block.chainid == 99801 || block.chainid == 31337, "Unsupported deployment chain");
        vm.startBroadcast();
        PoolFactory factory = new PoolFactory();
        WrappedHype hype = new WrappedHype();
        DemoToken dollar = new DemoToken();
        vm.stopBroadcast();
        console2.log("factory", address(factory));
        console2.log("wrapped HYPE (this deployment)", address(hype));
        console2.log("demo dollar (faucet token)", address(dollar));
    }
}
