// SPDX-License-Identifier: GPL-3.0-only
pragma solidity ^0.8.24;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import {LinearBondingMarket} from "./LinearBondingMarket.sol";

/// @notice Creation atomically deposits the caller's token allocation. No HYPE seed required.
contract CurveFactory is ReentrancyGuard {
    using SafeERC20 for IERC20;

    address[] public markets;
    mapping(address market => bool) public isMarket;

    event MarketCreated(address indexed market, address indexed manager, address indexed token);

    function createMarket(
        address token,
        uint256 allocation,
        uint256 startPrice,
        uint256 endPrice,
        uint16 feeBps,
        uint16 minFeeBps,
        uint16 maxFeeBps
    ) external nonReentrant returns (LinearBondingMarket market) {
        market = new LinearBondingMarket(
            token, msg.sender, allocation, startPrice, endPrice, feeBps, minFeeBps, maxFeeBps
        );
        uint256 beforeSender = IERC20(token).balanceOf(msg.sender);
        IERC20(token).safeTransferFrom(msg.sender, address(market), allocation);
        if (
            IERC20(token).balanceOf(address(market)) != allocation
                || IERC20(token).balanceOf(msg.sender) != beforeSender - allocation
        ) {
            revert LinearBondingMarket.UnsupportedTransfer();
        }
        markets.push(address(market));
        isMarket[address(market)] = true;
        emit MarketCreated(address(market), msg.sender, token);
    }

    function marketCount() external view returns (uint256) {
        return markets.length;
    }
}
