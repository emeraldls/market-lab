// SPDX-License-Identifier: GPL-3.0-only
pragma solidity ^0.8.24;

import {Script, console2} from "forge-std/Script.sol";
import {CurveFactory} from "../src/curves/CurveFactory.sol";

contract DeployCurves is Script {
    function run() external {
        require(block.chainid == 99801 || block.chainid == 31337, "Unsupported deployment chain");
        vm.startBroadcast();
        CurveFactory factory = new CurveFactory();
        vm.stopBroadcast();
        console2.log("curve factory", address(factory));
    }
}
