// SPDX-License-Identifier: GPL-3.0-only
pragma solidity ^0.8.24;

import {ERC20, IERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {Ownable2Step} from "@openzeppelin/contracts/access/Ownable2Step.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import {Math} from "@openzeppelin/contracts/utils/math/Math.sol";

/// @notice Two-token constant-product pool. All swap fees remain with liquidity providers.
contract MarketLabPool is ERC20, Ownable2Step, ReentrancyGuard {
    using SafeERC20 for IERC20;

    uint256 public constant BPS = 10_000;
    uint256 public constant MINIMUM_LIQUIDITY = 1_000;
    uint16 public constant MAX_FEE_BPS = 1_000;

    IERC20 public immutable token0;
    IERC20 public immutable token1;
    uint16 public immutable minFeeBps;
    uint16 public immutable maxFeeBps;

    uint16 public feeBps;
    address public operator;

    error InvalidTokens();
    error InvalidAmount();
    error InvalidRecipient();
    error InvalidFee();
    error Expired();
    error Slippage();
    error PoolNotSeeded();
    error OnlyManagerCanSeed();
    error UnauthorizedOperator();
    error UnsupportedTransfer();
    error ReserveLimit();

    event LiquidityAdded(
        address indexed sender,
        address indexed recipient,
        uint256 amount0,
        uint256 amount1,
        uint256 shares
    );
    event LiquidityRemoved(
        address indexed sender,
        address indexed recipient,
        uint256 amount0,
        uint256 amount1,
        uint256 shares
    );
    event Swap(
        address indexed sender,
        address indexed recipient,
        address indexed tokenIn,
        uint256 amountIn,
        uint256 amountOut,
        uint256 feeAmount
    );
    event ReservesUpdated(uint256 reserve0, uint256 reserve1);
    event FeeUpdated(address indexed sender, uint16 previousFeeBps, uint16 feeBps);
    event OperatorUpdated(address indexed previousOperator, address indexed operator);

    constructor(
        address tokenA,
        address tokenB,
        address manager,
        uint16 initialFeeBps,
        uint16 minimumFeeBps,
        uint16 maximumFeeBps
    ) ERC20("MarketLab Pool Share", "MLP") Ownable(manager) {
        if (tokenA == tokenB || tokenA.code.length == 0 || tokenB.code.length == 0) {
            revert InvalidTokens();
        }
        if (
            minimumFeeBps > initialFeeBps || initialFeeBps > maximumFeeBps
                || maximumFeeBps > MAX_FEE_BPS
        ) revert InvalidFee();

        (address first, address second) = tokenA < tokenB ? (tokenA, tokenB) : (tokenB, tokenA);
        token0 = IERC20(first);
        token1 = IERC20(second);
        minFeeBps = minimumFeeBps;
        maxFeeBps = maximumFeeBps;
        feeBps = initialFeeBps;
    }

    function getReserves() public view returns (uint256 reserve0, uint256 reserve1) {
        reserve0 = token0.balanceOf(address(this));
        reserve1 = token1.balanceOf(address(this));
    }

    /// @notice Maximum inputs are in token0/token1 order. Only the required amounts are pulled.
    function previewDeposit(uint256 max0, uint256 max1)
        public
        view
        returns (uint256 amount0, uint256 amount1, uint256 shares)
    {
        if (max0 == 0 || max1 == 0) revert InvalidAmount();
        (uint256 reserve0, uint256 reserve1) = getReserves();
        uint256 supply = totalSupply();

        if (supply == 0) {
            _checkReserves(max0, max1);
            _checkReserves(reserve0 + max0, reserve1 + max1);
            uint256 initial = Math.sqrt((reserve0 + max0) * (reserve1 + max1));
            if (initial <= MINIMUM_LIQUIDITY) revert InvalidAmount();
            return (max0, max1, initial - MINIMUM_LIQUIDITY);
        }

        if (reserve0 == 0 || reserve1 == 0) revert InvalidAmount();
        shares = Math.min(Math.mulDiv(max0, supply, reserve0), Math.mulDiv(max1, supply, reserve1));
        if (shares == 0) revert InvalidAmount();
        amount0 = Math.mulDiv(shares, reserve0, supply, Math.Rounding.Ceil);
        amount1 = Math.mulDiv(shares, reserve1, supply, Math.Rounding.Ceil);
        _checkReserves(reserve0 + amount0, reserve1 + amount1);
    }

    function deposit(
        uint256 max0,
        uint256 max1,
        uint256 minShares,
        address recipient,
        uint256 deadline
    ) external nonReentrant returns (uint256 amount0, uint256 amount1, uint256 shares) {
        _checkRequest(recipient, deadline);
        bool initial = totalSupply() == 0;
        if (initial && msg.sender != owner()) revert OnlyManagerCanSeed();
        (amount0, amount1, shares) = previewDeposit(max0, max1);
        if (minShares == 0 || shares < minShares) revert Slippage();

        _pullExact(token0, amount0);
        _pullExact(token1, amount1);

        // Permanently lock the initial dust shares to make share-price inflation costly.
        if (initial) _mint(address(1), MINIMUM_LIQUIDITY);
        _mint(recipient, shares);
        _emitReserves();
        emit LiquidityAdded(msg.sender, recipient, amount0, amount1, shares);
    }

    function previewWithdraw(uint256 shares)
        public
        view
        returns (uint256 amount0, uint256 amount1)
    {
        uint256 supply = totalSupply();
        if (shares == 0 || shares > supply) revert InvalidAmount();
        (uint256 reserve0, uint256 reserve1) = getReserves();
        amount0 = Math.mulDiv(shares, reserve0, supply);
        amount1 = Math.mulDiv(shares, reserve1, supply);
    }

    function withdraw(
        uint256 shares,
        uint256 min0,
        uint256 min1,
        address recipient,
        uint256 deadline
    ) external nonReentrant returns (uint256 amount0, uint256 amount1) {
        _checkRequest(recipient, deadline);
        (amount0, amount1) = previewWithdraw(shares);
        if (amount0 == 0 && amount1 == 0) revert InvalidAmount();
        if (amount0 < min0 || amount1 < min1) revert Slippage();
        _burn(msg.sender, shares);
        if (amount0 != 0) _pushExact(token0, recipient, amount0);
        if (amount1 != 0) _pushExact(token1, recipient, amount1);
        _emitReserves();
        emit LiquidityRemoved(msg.sender, recipient, amount0, amount1, shares);
    }

    function quoteSwap(address tokenIn, uint256 amountIn)
        public
        view
        returns (uint256 amountOut, uint256 feeAmount)
    {
        if (totalSupply() == 0) revert PoolNotSeeded();
        if (tokenIn != address(token0) && tokenIn != address(token1)) revert InvalidTokens();
        if (amountIn == 0 || amountIn > type(uint112).max) revert InvalidAmount();
        (uint256 reserve0, uint256 reserve1) = getReserves();
        _checkReserves(reserve0, reserve1);
        (uint256 reserveIn, uint256 reserveOut) =
            tokenIn == address(token0) ? (reserve0, reserve1) : (reserve1, reserve0);
        if (reserveIn + amountIn > type(uint112).max) revert ReserveLimit();

        // Charge an integer fee in the input token; rounding can add at most one base unit.
        feeAmount = Math.mulDiv(amountIn, feeBps, BPS, Math.Rounding.Ceil);
        uint256 netInput = amountIn - feeAmount;
        amountOut = Math.mulDiv(netInput, reserveOut, reserveIn + netInput);
        if (amountOut == 0) revert InvalidAmount();
    }

    function swapExactInput(
        address tokenIn,
        uint256 amountIn,
        uint256 minAmountOut,
        address recipient,
        uint256 deadline
    ) external nonReentrant returns (uint256 amountOut) {
        _checkRequest(recipient, deadline);
        uint256 feeAmount;
        (amountOut, feeAmount) = quoteSwap(tokenIn, amountIn);
        if (minAmountOut == 0 || amountOut < minAmountOut) revert Slippage();
        IERC20 tokenOut = tokenIn == address(token0) ? token1 : token0;
        _pullExact(IERC20(tokenIn), amountIn);
        _pushExact(tokenOut, recipient, amountOut);
        _emitReserves();
        emit Swap(msg.sender, recipient, tokenIn, amountIn, amountOut, feeAmount);
    }

    /// @notice A zero operator disables delegated fee updates without affecting withdrawals.
    function setOperator(address nextOperator) external nonReentrant onlyOwner {
        address previousOperator = operator;
        operator = nextOperator;
        emit OperatorUpdated(previousOperator, nextOperator);
    }

    function setFee(uint16 nextFeeBps) external nonReentrant {
        if (msg.sender != operator && msg.sender != owner()) revert UnauthorizedOperator();
        if (nextFeeBps < minFeeBps || nextFeeBps > maxFeeBps) revert InvalidFee();
        uint16 previousFeeBps = feeBps;
        feeBps = nextFeeBps;
        emit FeeUpdated(msg.sender, previousFeeBps, nextFeeBps);
    }

    function _pullExact(IERC20 token, uint256 amount) private {
        uint256 beforeBalance = token.balanceOf(address(this));
        token.safeTransferFrom(msg.sender, address(this), amount);
        if (token.balanceOf(address(this)) != beforeBalance + amount) revert UnsupportedTransfer();
    }

    function _pushExact(IERC20 token, address recipient, uint256 amount) private {
        uint256 beforePool = token.balanceOf(address(this));
        uint256 beforeRecipient = token.balanceOf(recipient);
        token.safeTransfer(recipient, amount);
        if (
            token.balanceOf(address(this)) != beforePool - amount
                || token.balanceOf(recipient) != beforeRecipient + amount
        ) revert UnsupportedTransfer();
    }

    function _checkRequest(address recipient, uint256 deadline) private view {
        if (block.timestamp > deadline) revert Expired();
        if (recipient == address(0) || recipient == address(this)) revert InvalidRecipient();
    }

    function _checkReserves(uint256 reserve0, uint256 reserve1) private pure {
        if (reserve0 > type(uint112).max || reserve1 > type(uint112).max) revert ReserveLimit();
    }

    function _emitReserves() private {
        (uint256 reserve0, uint256 reserve1) = getReserves();
        emit ReservesUpdated(reserve0, reserve1);
    }
}
