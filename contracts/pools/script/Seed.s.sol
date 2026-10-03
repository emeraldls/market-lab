// SPDX-License-Identifier: GPL-3.0-only
pragma solidity ^0.8.24;

import {Script, console2} from "forge-std/Script.sol";
import {MarketLabPool} from "../src/MarketLabPool.sol";
import {PoolFactory} from "../src/PoolFactory.sol";
import {WrappedHype} from "../src/WrappedHype.sol";
import {DemoToken} from "./DemoToken.sol";

/// @notice Create a demo pool. Amounts are HYPE wei and DEMO base units (6 decimals).
contract Seed is Script {
    function run(uint256 hypeAmount, uint256 demoAmount) external returns (MarketLabPool pool) {
        require(block.chainid == 99801, "Seed requires Elysium chain 99801");
        require(hypeAmount > 0 && demoAmount > 0, "Both seed amounts must be positive");

        string memory deployment =
            vm.readFile(string.concat(vm.projectRoot(), "/deployments/99801.json"));

        PoolFactory factory =
            PoolFactory(vm.parseJsonAddress(deployment, ".contracts.PoolFactory.address"));

        WrappedHype hype =
            WrappedHype(payable(vm.parseJsonAddress(deployment, ".contracts.WrappedHype.address")));

        DemoToken demo = DemoToken(vm.parseJsonAddress(deployment, ".contracts.DemoToken.address"));

        vm.startBroadcast();

        pool = factory.createPool(address(hype), address(demo), 30, 5, 100);

        address manager = pool.owner();

        (uint256 amount0, uint256 amount1) = address(pool.token0()) == address(hype)
            ? (hypeAmount, demoAmount)
            : (demoAmount, hypeAmount);

        (,, uint256 shares) = pool.previewDeposit(amount0, amount1);

        hype.deposit{value: hypeAmount}();
        demo.mint(manager, demoAmount);

        hype.approve(address(pool), hypeAmount);
        demo.approve(address(pool), demoAmount);

        pool.deposit(amount0, amount1, shares, manager, block.timestamp + 1 hours);
        vm.stopBroadcast();

        // These also run without --broadcast, checking the complete proposed transaction sequence.
        (uint256 reserve0, uint256 reserve1) = pool.getReserves();
        
        assert(factory.isPool(address(pool)));
        assert(reserve0 == amount0 && reserve1 == amount1);
        assert(pool.balanceOf(manager) == shares);
        assert(pool.feeBps() == 30 && pool.minFeeBps() == 5 && pool.maxFeeBps() == 100);
        assert(pool.operator() == address(0));
        assert(hype.allowance(manager, address(pool)) == 0);
        assert(demo.allowance(manager, address(pool)) == 0);

        console2.log("pool", address(pool));
        console2.log("manager", manager);
        console2.log("HYPE deposited (wei)", hypeAmount);
        console2.log("DEMO deposited (6 decimals)", demoAmount);
        console2.log("LP shares (base units)", shares);
    }
}
