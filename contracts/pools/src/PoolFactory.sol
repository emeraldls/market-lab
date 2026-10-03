// SPDX-License-Identifier: GPL-3.0-only
pragma solidity ^0.8.24;

import {MarketLabPool} from "./MarketLabPool.sol";

/// @notice Permissionless catalog. Multiple managers may create pools for the same pair.
contract PoolFactory {
    address[] public pools;
    mapping(address pool => bool) public isPool;

    event PoolCreated(
        address indexed pool,
        address indexed manager,
        address token0,
        address token1,
        uint16 minFeeBps,
        uint16 maxFeeBps
    );

    function createPool(
        address tokenA,
        address tokenB,
        uint16 initialFeeBps,
        uint16 minFeeBps,
        uint16 maxFeeBps
    ) external returns (MarketLabPool pool) {
        pool = new MarketLabPool(tokenA, tokenB, msg.sender, initialFeeBps, minFeeBps, maxFeeBps);
        pools.push(address(pool));
        isPool[address(pool)] = true;
        emit PoolCreated(
            address(pool),
            msg.sender,
            address(pool.token0()),
            address(pool.token1()),
            minFeeBps,
            maxFeeBps
        );
    }

    function poolCount() external view returns (uint256) {
        return pools.length;
    }
}
