// SPDX-License-Identifier: GPL-3.0-only
pragma solidity ^0.8.24;

import {Script, console2} from "forge-std/Script.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {PoolFactory} from "../src/PoolFactory.sol";
import {MarketLabPool} from "../src/MarketLabPool.sol";

/// @notice Create and seed any supported ERC-20 pair using the caller's existing balances.
contract CreatePool is Script {
    using SafeERC20 for IERC20;

    function run(
        address tokenA,
        address tokenB,
        uint256 amountA,
        uint256 amountB,
        uint16 initialFeeBps,
        uint16 minFeeBps,
        uint16 maxFeeBps
    ) external returns (MarketLabPool pool) {
        require(block.chainid == 99801, "Requires Elysium chain 99801");
        require(amountA > 0 && amountB > 0, "Both seed amounts must be positive");
        string memory deployment =
            vm.readFile(string.concat(vm.projectRoot(), "/deployments/99801.json"));
        PoolFactory factory =
            PoolFactory(vm.parseJsonAddress(deployment, ".contracts.PoolFactory.address"));

        vm.startBroadcast();
        pool = factory.createPool(tokenA, tokenB, initialFeeBps, minFeeBps, maxFeeBps);
        uint256 shares = seed(pool, tokenA, tokenB, amountA, amountB);
        vm.stopBroadcast();

        require(factory.isPool(address(pool)), "Pool was not registered");
        console2.log("pool", address(pool));
        console2.log("manager", pool.owner());
        console2.log("LP shares (base units)", shares);
    }

    function seed(
        MarketLabPool pool,
        address tokenA,
        address tokenB,
        uint256 amountA,
        uint256 amountB
    ) private returns (uint256 shares) {
        address manager = pool.owner();
        require(IERC20(tokenA).balanceOf(manager) >= amountA, "Insufficient token A balance");
        require(IERC20(tokenB).balanceOf(manager) >= amountB, "Insufficient token B balance");

        (uint256 amount0, uint256 amount1) =
            address(pool.token0()) == tokenA ? (amountA, amountB) : (amountB, amountA);
        (,, shares) = pool.previewDeposit(amount0, amount1);
        
        IERC20(tokenA).forceApprove(address(pool), amountA);
        IERC20(tokenB).forceApprove(address(pool), amountB);
        pool.deposit(amount0, amount1, shares, manager, block.timestamp + 1 hours);
        require(pool.balanceOf(manager) == shares, "Unexpected LP shares");
    }
}
