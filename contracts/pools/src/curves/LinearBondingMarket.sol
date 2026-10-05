// SPDX-License-Identifier: GPL-3.0-only
pragma solidity ^0.8.24;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {IERC20Metadata} from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {Ownable2Step} from "@openzeppelin/contracts/access/Ownable2Step.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import {Math} from "@openzeppelin/contracts/utils/math/Math.sol";

/// @notice Fixed inventory, two-way linear market. Prices are HYPE wei per whole token.
/// @dev Standard, non-rebasing ERC20 tokens only. No reserve withdrawal or migration in v1.
contract LinearBondingMarket is Ownable2Step, ReentrancyGuard {
    using SafeERC20 for IERC20;

    uint256 public constant BPS = 10_000;
    uint16 public constant MAX_FEE_BPS = 1_000;
    IERC20 public immutable token;
    uint256 public immutable tokenUnit;
    uint256 public immutable allocation;
    uint256 public immutable startPrice;
    uint256 public immutable endPrice;
    uint16 public immutable minFeeBps;
    uint16 public immutable maxFeeBps;
    uint16 public feeBps;
    address public operator;
    uint256 public sold;
    uint256 public accruedFees;

    error InvalidConfiguration();
    error InvalidAmount();
    error InvalidRecipient();
    error InvalidFee();
    error UnauthorizedOperator();
    error Expired();
    error Slippage();
    error UnsupportedTransfer();
    error NativeTransferFailed();

    event Buy(
        address indexed sender, address indexed recipient, uint256 tokens, uint256 hype, uint256 fee
    );
    event Sell(
        address indexed sender, address indexed recipient, uint256 tokens, uint256 hype, uint256 fee
    );
    event FeesClaimed(address indexed recipient, uint256 amount);
    event OperatorUpdated(address indexed previousOperator, address indexed operator);
    event FeeUpdated(address indexed sender, uint16 previousFeeBps, uint16 feeBps);

    constructor(
        address asset,
        address manager,
        uint256 inventory,
        uint256 initialPrice,
        uint256 finalPrice,
        uint16 initialFee,
        uint16 minimumFee,
        uint16 maximumFee
    ) Ownable(manager) {
        if (
            asset.code.length == 0 || inventory == 0 || inventory > type(uint96).max
                || initialPrice == 0 || finalPrice < initialPrice || finalPrice > type(uint96).max
        ) {
            revert InvalidConfiguration();
        }
        uint8 decimals = IERC20Metadata(asset).decimals();
        if (decimals > 18) revert InvalidConfiguration();
        if (minimumFee > initialFee || initialFee > maximumFee || maximumFee > MAX_FEE_BPS) {
            revert InvalidFee();
        }
        token = IERC20(asset);
        tokenUnit = 10 ** decimals;
        allocation = inventory;
        startPrice = initialPrice;
        endPrice = finalPrice;
        feeBps = initialFee;
        minFeeBps = minimumFee;
        maxFeeBps = maximumFee;
        if (backingAt(inventory) == 0) revert InvalidConfiguration();
    }

    /// @notice Integral of the linear price, rounded down, in HYPE wei.
    /// @dev Trades use differences of this potential, so rounding telescopes across trades.
    function backingAt(uint256 supply) public view returns (uint256) {
        if (supply > allocation) revert InvalidAmount();
        return Math.mulDiv(startPrice, supply, tokenUnit)
            + Math.mulDiv(endPrice - startPrice, supply * supply, 2 * allocation * tokenUnit);
    }

    function backing() external view returns (uint256) {
        return backingAt(sold);
    }

    function currentPrice() external view returns (uint256) {
        return startPrice + Math.mulDiv(endPrice - startPrice, sold, allocation);
    }

    /// @notice Exact-token-output quote, including a fee deducted from the gross HYPE payment.
    function quoteBuy(uint256 tokens) public view returns (uint256 hype, uint256 fee) {
        if (tokens == 0 || tokens > allocation - sold) revert InvalidAmount();
        uint256 cost = backingAt(sold + tokens) - backingAt(sold);
        if (cost == 0) revert InvalidAmount();
        hype = Math.mulDiv(cost, BPS, BPS - feeBps, Math.Rounding.Ceil);
        fee = hype - cost;
    }

    /// @notice Largest affordable token amount; unspent HYPE is refunded by buy().
    function quoteBuyHype(uint256 budget)
        public
        view
        returns (uint256 tokens, uint256 hype, uint256 fee)
    {
        if (budget == 0) revert InvalidAmount();
        uint256 base = backingAt(sold);
        uint256 net = Math.mulDiv(budget, BPS - feeBps, BPS);
        uint256 low;
        uint256 high = allocation - sold;
        while (low < high) {
            uint256 mid = low + (high - low + 1) / 2;
            if (backingAt(sold + mid) - base <= net) low = mid;
            else high = mid - 1;
        }
        tokens = low;
        (hype, fee) = quoteBuy(tokens);
    }

    function quoteSell(uint256 tokens) public view returns (uint256 hype, uint256 fee) {
        if (tokens == 0 || tokens > sold) revert InvalidAmount();
        uint256 gross = backingAt(sold) - backingAt(sold - tokens);
        fee = Math.mulDiv(gross, feeBps, BPS, Math.Rounding.Ceil);
        hype = gross - fee;
        if (hype == 0) revert InvalidAmount();
    }

    /// @notice Buy exactly tokens; msg.value is the maximum spend, with excess returned to sender.
    function buy(uint256 tokens, address recipient, uint256 deadline)
        external
        payable
        nonReentrant
    {
        _checkRequest(recipient, deadline);
        _buy(tokens, recipient);
    }

    /// @notice Spend up to msg.value at execution-time prices, enforcing a minimum token output.
    function buyHype(uint256 minimumTokens, address recipient, uint256 deadline)
        external
        payable
        nonReentrant
    {
        _checkRequest(recipient, deadline);
        (uint256 tokens,,) = quoteBuyHype(msg.value);
        if (minimumTokens == 0 || tokens < minimumTokens) revert Slippage();
        _buy(tokens, recipient);
    }

    function _buy(uint256 tokens, address recipient) private {
        (uint256 hype, uint256 fee) = quoteBuy(tokens);
        if (msg.value < hype) revert Slippage();
        sold += tokens;
        accruedFees += fee;
        uint256 beforeMarket = token.balanceOf(address(this));
        uint256 beforeRecipient = token.balanceOf(recipient);
        token.safeTransfer(recipient, tokens);
        if (
            token.balanceOf(address(this)) != beforeMarket - tokens
                || token.balanceOf(recipient) != beforeRecipient + tokens
        ) revert UnsupportedTransfer();
        if (msg.value > hype) _sendHype(msg.sender, msg.value - hype);
        emit Buy(msg.sender, recipient, tokens, hype, fee);
    }

    function sell(uint256 tokens, uint256 minimumHype, address recipient, uint256 deadline)
        external
        nonReentrant
    {
        _checkRequest(recipient, deadline);
        (uint256 hype, uint256 fee) = quoteSell(tokens);
        if (minimumHype == 0 || hype < minimumHype) revert Slippage();
        sold -= tokens;
        accruedFees += fee;
        uint256 beforeMarket = token.balanceOf(address(this));
        uint256 beforeSender = token.balanceOf(msg.sender);
        token.safeTransferFrom(msg.sender, address(this), tokens);
        if (
            token.balanceOf(address(this)) != beforeMarket + tokens
                || token.balanceOf(msg.sender) != beforeSender - tokens
        ) revert UnsupportedTransfer();
        _sendHype(recipient, hype);
        emit Sell(msg.sender, recipient, tokens, hype, fee);
    }

    function claimFees(address recipient) external nonReentrant onlyOwner {
        _checkRequest(recipient, block.timestamp);
        uint256 amount = accruedFees;
        if (amount == 0) revert InvalidAmount();
        accruedFees = 0;
        _sendHype(recipient, amount);
        emit FeesClaimed(recipient, amount);
    }

    function setOperator(address nextOperator) external nonReentrant onlyOwner {
        address previous = operator;
        operator = nextOperator;
        emit OperatorUpdated(previous, nextOperator);
    }

    function setFee(uint16 nextFee) external nonReentrant {
        if (msg.sender != operator && msg.sender != owner()) revert UnauthorizedOperator();
        if (nextFee < minFeeBps || nextFee > maxFeeBps) revert InvalidFee();
        uint16 previous = feeBps;
        feeBps = nextFee;
        emit FeeUpdated(msg.sender, previous, nextFee);
    }

    function _checkRequest(address recipient, uint256 deadline) private view {
        if (block.timestamp > deadline) revert Expired();
        if (recipient == address(0) || recipient == address(this)) revert InvalidRecipient();
    }

    function _sendHype(address recipient, uint256 amount) private {
        (bool success,) = recipient.call{value: amount}("");
        if (!success) revert NativeTransferFailed();
    }
}
