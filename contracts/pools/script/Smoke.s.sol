// SPDX-License-Identifier: GPL-3.0-only
pragma solidity ^0.8.24;

import {Script, console2} from "forge-std/Script.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {IERC20Errors} from "@openzeppelin/contracts/interfaces/draft-IERC6093.sol";
import {MarketLabPool} from "../src/MarketLabPool.sol";
import {PoolFactory} from "../src/PoolFactory.sol";
import {WrappedHype} from "../src/WrappedHype.sol";
import {DemoToken} from "./DemoToken.sol";

/// @notice Runnable local flow with accounting and permission checks, not a public deployment.
contract Smoke is Script {
    string private constant MNEMONIC =
        "test test test test test test test test test test test junk";

    PoolFactory private factory;
    MarketLabPool private pool;
    WrappedHype private hype;
    DemoToken private dollar;
    uint256 private managerKey;
    uint256 private lpKey;
    uint256 private traderKey;
    uint256 private operatorKey;
    uint256 private deadline;

    function run() external {
        require(block.chainid == 31337, "Smoke only runs on local chain 31337");
        managerKey = vm.deriveKey(MNEMONIC, 0);
        lpKey = vm.deriveKey(MNEMONIC, 1);
        traderKey = vm.deriveKey(MNEMONIC, 2);
        operatorKey = vm.deriveKey(MNEMONIC, 3);
        deadline = block.timestamp + 1 hours;
        vm.deal(vm.addr(managerKey), 10_000 ether);
        vm.deal(vm.addr(lpKey), 10_000 ether);
        vm.deal(vm.addr(traderKey), 10_000 ether);
        vm.deal(vm.addr(operatorKey), 10_000 ether);

        _create();
        _deposit(managerKey, 100 ether, 10_000e6);
        uint256 lpShares = _deposit(lpKey, 10 ether, 1_000e6);
        _checkRequests();
        _trade();
        _changeFee();
        _withdraw(lpShares);
        _checkDonationExit();

        assert(hype.totalSupply() == address(hype).balance);
        console2.log("PASS: create, two LPs, swap, fee update, revoke operator, withdraw");
        console2.log("factory", address(factory));
        console2.log("pool", address(pool));
        console2.log("wrapped HYPE", address(hype));
        console2.log("demo dollar", address(dollar));
    }

    function _create() private {
        vm.startBroadcast(managerKey);
        factory = new PoolFactory();
        hype = new WrappedHype();
        dollar = new DemoToken();
        pool = factory.createPool(address(hype), address(dollar), 30, 5, 100);
        vm.stopBroadcast();

        assert(factory.isPool(address(pool)) && factory.poolCount() == 1);
        assert(pool.owner() == vm.addr(managerKey));
        assert(address(pool.token0()) < address(pool.token1()));
        _mustRevert(
            vm.addr(lpKey),
            address(pool),
            abi.encodeCall(pool.deposit, (1 ether, 1 ether, 1, vm.addr(lpKey), deadline)),
            MarketLabPool.OnlyManagerCanSeed.selector
        );
    }

    function _deposit(uint256 key, uint256 hypeAmount, uint256 dollarAmount)
        private
        returns (uint256 shares)
    {
        address account = vm.addr(key);
        (uint256 amount0, uint256 amount1) = address(pool.token0()) == address(hype)
            ? (hypeAmount, dollarAmount)
            : (dollarAmount, hypeAmount);
        (,, shares) = pool.previewDeposit(amount0, amount1);
        vm.startBroadcast(key);
        hype.deposit{value: hypeAmount}();
        dollar.mint(account, dollarAmount);
        hype.approve(address(pool), hypeAmount);
        dollar.approve(address(pool), dollarAmount);
        pool.deposit(amount0, amount1, shares, account, deadline);
        vm.stopBroadcast();
        assert(pool.balanceOf(account) == shares);
        console2.log("LP shares minted", shares);
    }

    function _checkRequests() private {
        address trader = vm.addr(traderKey);
        _mustRevert(
            trader,
            address(pool),
            abi.encodeCall(pool.setFee, (50)),
            MarketLabPool.UnauthorizedOperator.selector
        );
        _mustRevert(
            trader,
            address(pool),
            abi.encodeCall(pool.withdraw, (1_000_000, 0, 0, trader, deadline)),
            IERC20Errors.ERC20InsufficientBalance.selector
        );
        _mustRevert(
            trader,
            address(pool),
            abi.encodeCall(pool.swapExactInput, (address(hype), 1 ether, 1, trader, 0)),
            MarketLabPool.Expired.selector
        );
        _mustRevert(
            trader,
            address(pool),
            abi.encodeCall(
                pool.swapExactInput, (address(hype), 1 ether, type(uint256).max, trader, deadline)
            ),
            MarketLabPool.Slippage.selector
        );
        _mustRevert(
            trader,
            address(pool),
            abi.encodeCall(pool.deposit, (1 ether, 1 ether, type(uint256).max, trader, deadline)),
            MarketLabPool.Slippage.selector
        );
    }

    function _trade() private {
        address trader = vm.addr(traderKey);
        (uint256 reserve0, uint256 reserve1) = pool.getReserves();
        (uint256 output, uint256 fee) = pool.quoteSwap(address(hype), 1 ether);
        assert(fee == 0.003 ether);
        vm.startBroadcast(traderKey);
        hype.deposit{value: 1 ether}();
        hype.approve(address(pool), 1 ether);
        pool.swapExactInput(address(hype), 1 ether, output, trader, deadline);
        vm.stopBroadcast();
        assert(dollar.balanceOf(trader) == output);
        (uint256 next0, uint256 next1) = pool.getReserves();
        assert(next0 * next1 > reserve0 * reserve1);

        // Exercise the opposite direction too; fees stay in whichever token was sold.
        uint256 amountIn = output / 2;
        (uint256 returnHype,) = pool.quoteSwap(address(dollar), amountIn);
        vm.startBroadcast(traderKey);
        dollar.approve(address(pool), amountIn);
        pool.swapExactInput(address(dollar), amountIn, returnHype, trader, deadline);
        vm.stopBroadcast();
        assert(hype.balanceOf(trader) == returnHype);
    }

    function _changeFee() private {
        address operator = vm.addr(operatorKey);
        vm.startBroadcast(managerKey);
        pool.setOperator(operator);
        vm.stopBroadcast();
        (uint256 before0, uint256 before1) = pool.getReserves();
        vm.startBroadcast(operatorKey);
        pool.setFee(50);
        vm.stopBroadcast();
        (uint256 after0, uint256 after1) = pool.getReserves();
        assert(before0 == after0 && before1 == after1 && pool.feeBps() == 50);
        _mustRevert(
            operator,
            address(pool),
            abi.encodeCall(pool.setFee, (101)),
            MarketLabPool.InvalidFee.selector
        );
        _mustRevert(
            operator,
            address(pool),
            abi.encodeCall(pool.withdraw, (1_000_000, 0, 0, operator, deadline)),
            IERC20Errors.ERC20InsufficientBalance.selector
        );
        vm.startBroadcast(managerKey);
        pool.setOperator(address(0));
        vm.stopBroadcast();
        _mustRevert(
            operator,
            address(pool),
            abi.encodeCall(pool.setFee, (30)),
            MarketLabPool.UnauthorizedOperator.selector
        );
    }

    function _withdraw(uint256 shares) private {
        address lp = vm.addr(lpKey);
        (uint256 amount0, uint256 amount1) = pool.previewWithdraw(shares);
        vm.startBroadcast(lpKey);
        pool.withdraw(shares, amount0, amount1, lp, deadline);
        vm.stopBroadcast();
        assert(pool.balanceOf(lp) == 0);
        assert(pool.token0().balanceOf(lp) == amount0);
        assert(pool.token1().balanceOf(lp) == amount1);
        uint256 hypeAmount = hype.balanceOf(lp);
        uint256 backing = address(hype).balance;
        vm.startBroadcast(lpKey);
        hype.withdraw(hypeAmount);
        vm.stopBroadcast();
        assert(hype.balanceOf(lp) == 0 && address(hype).balance == backing - hypeAmount);
    }

    function _mustRevert(address caller, address target, bytes memory data, bytes4 selector)
        private
    {
        uint256 snapshot = vm.snapshotState();
        vm.prank(caller);
        (bool success, bytes memory result) = target.call(data);
        // Rejected simulation calls must not consume nonces in the broadcast sequence.
        require(vm.revertToStateAndDelete(snapshot), "Could not restore simulation");
        require(
            !success && result.length >= 4 && bytes4(result) == selector,
            "Missing expected rejection"
        );
    }

    function _checkDonationExit() private {
        uint256 snapshot = vm.snapshotState();
        dollar.mint(address(pool), uint256(type(uint112).max) + 1);
        address manager = vm.addr(managerKey);
        uint256 shares = pool.balanceOf(manager);
        vm.prank(manager);
        pool.withdraw(shares, 0, 0, manager, deadline);
        assert(pool.balanceOf(manager) == 0);
        require(vm.revertToStateAndDelete(snapshot), "Could not restore simulation");
    }
}
