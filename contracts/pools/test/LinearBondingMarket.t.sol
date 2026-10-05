// SPDX-License-Identifier: GPL-3.0-only
pragma solidity ^0.8.24;

import {Test} from "forge-std/Test.sol";
import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {CurveFactory} from "../src/curves/CurveFactory.sol";
import {LinearBondingMarket} from "../src/curves/LinearBondingMarket.sol";

contract CurveToken is ERC20 {
    uint8 private immutable precision;
    bool public taxed;

    constructor(uint8 decimals_) ERC20("Curve token", "CURVE") {
        precision = decimals_;
    }

    function decimals() public view override returns (uint8) {
        return precision;
    }

    function mint(address account, uint256 amount) external {
        _mint(account, amount);
    }

    function setTaxed(bool enabled) external {
        taxed = enabled;
    }

    function _update(address from, address to, uint256 value) internal override {
        if (taxed && from != address(0) && to != address(0)) {
            super._update(from, address(0), value / 100);
            value -= value / 100;
        }
        super._update(from, to, value);
    }
}

contract RejectHype {
    function buy(LinearBondingMarket market, uint256 amount) external payable {
        market.buy{value: msg.value}(amount, address(this), block.timestamp);
    }
}

contract ReenterCurve {
    LinearBondingMarket public market;
    bool public succeeded;

    constructor(LinearBondingMarket market_) {
        market = market_;
    }

    receive() external payable {
        (succeeded,) = address(market).call(abi.encodeCall(market.claimFees, (address(this))));
    }
}

contract LinearBondingMarketTest is Test {
    CurveToken token;
    CurveFactory factory;
    LinearBondingMarket market;
    address alice = makeAddr("alice");
    address operator = makeAddr("operator");
    uint256 constant ALLOCATION = 1_000 ether;

    receive() external payable {}

    function setUp() public {
        factory = new CurveFactory();
        token = new CurveToken(18);
        token.mint(address(this), ALLOCATION);
        token.approve(address(factory), ALLOCATION);
        market =
            factory.createMarket(address(token), ALLOCATION, 0.001 ether, 0.003 ether, 30, 5, 100);
        vm.deal(alice, 100 ether);
        vm.prank(alice);
        token.approve(address(market), type(uint256).max);
    }

    function buy(uint256 amount) internal returns (uint256 cost, uint256 fee) {
        (cost, fee) = market.quoteBuy(amount);
        vm.prank(alice);
        market.buy{value: cost}(amount, alice, block.timestamp);
    }

    function assertBacking() internal view {
        assertEq(address(market).balance, market.backing() + market.accruedFees());
        assertEq(token.balanceOf(address(market)), ALLOCATION - market.sold());
    }

    function testBudgetBuyUsesFullQuoteAndProtectsMinimum() public {
        (uint256 expected,,) = market.quoteBuyHype(0.1 ether);
        vm.prank(alice);
        vm.expectRevert(LinearBondingMarket.Slippage.selector);
        market.buyHype{value: 0.1 ether}(expected + 1, alice, block.timestamp);
        vm.prank(alice);
        market.buyHype{value: 0.1 ether}(expected, alice, block.timestamp);
        assertEq(token.balanceOf(alice), expected);
        assertBacking();
    }

    function testBudgetBuyRefundsExcessAtInventoryLimit() public {
        (uint256 cost,) = market.quoteBuy(ALLOCATION);
        uint256 before = alice.balance;
        vm.prank(alice);
        market.buyHype{value: cost + 1 ether}(ALLOCATION, alice, block.timestamp);
        assertEq(before - alice.balance, cost);
        assertEq(market.sold(), ALLOCATION);
        assertBacking();
    }

    function testFuzzCurveArithmeticBounds(
        uint96 inventory,
        uint96 start,
        uint96 finish,
        uint8 decimals
    ) public {
        uint256 cap = bound(uint256(inventory), 1 ether, type(uint96).max);
        uint256 p0 = bound(uint256(start), 1 ether, type(uint96).max);
        uint256 p1 = bound(uint256(finish), p0, type(uint96).max);
        CurveToken asset = new CurveToken(uint8(bound(decimals, 0, 18)));
        LinearBondingMarket curve =
            new LinearBondingMarket(address(asset), address(this), cap, p0, p1, 1000, 0, 1000);
        assertGt(curve.backingAt(cap), 0);
        (uint256 cost, uint256 fee) = curve.quoteBuy(cap);
        assertEq(cost - fee, curve.backingAt(cap));
        (uint256 tokens, uint256 spend,) = curve.quoteBuyHype(cost);
        assertEq(tokens, cap);
        assertEq(spend, cost);
    }

    function testFactoryAtomicallyFundsAndRegisters() public view {
        assertEq(factory.marketCount(), 1);
        assertEq(factory.markets(0), address(market));
        assertTrue(factory.isMarket(address(market)));
        assertEq(market.owner(), address(this));
        assertEq(market.currentPrice(), 0.001 ether);
        assertBacking();
    }

    function testIntegralAndFeeAccounting() public {
        (uint256 cost, uint256 fee) = buy(100 ether);
        assertEq(cost - fee, 0.11 ether);
        assertEq(market.currentPrice(), 0.0012 ether);
        assertEq(market.backing(), 0.11 ether);
        assertEq(market.accruedFees(), fee);
        assertBacking();
    }

    function testFullSellBackAfterFeesClaimed() public {
        buy(ALLOCATION);
        market.claimFees(address(this));
        (uint256 payout, uint256 fee) = market.quoteSell(ALLOCATION);
        uint256 before = alice.balance;
        vm.prank(alice);
        market.sell(ALLOCATION, payout, alice, block.timestamp);
        assertEq(alice.balance - before, payout);
        assertEq(market.sold(), 0);
        assertEq(market.backing(), 0);
        assertEq(market.accruedFees(), fee);
        assertBacking();
        market.claimFees(address(this));
        assertEq(address(market).balance, 0);
    }

    function testSoldOutMarketReopensOnSell() public {
        buy(ALLOCATION);
        vm.expectRevert(LinearBondingMarket.InvalidAmount.selector);
        market.quoteBuy(1 ether);
        (uint256 payout,) = market.quoteSell(10 ether);
        vm.prank(alice);
        market.sell(10 ether, payout, alice, block.timestamp);
        buy(10 ether);
        assertEq(market.sold(), ALLOCATION);
        assertBacking();
    }

    function testRefundsUnusedBudget() public {
        (uint256 tokens, uint256 cost, uint256 fee) = market.quoteBuyHype(0.1 ether);
        assertGt(tokens, 0);
        assertLe(cost, 0.1 ether);
        uint256 before = alice.balance;
        vm.prank(alice);
        market.buy{value: 0.2 ether}(tokens, alice, block.timestamp);
        assertEq(before - alice.balance, cost);
        assertEq(market.accruedFees(), fee);
        assertBacking();
    }

    function testPermissionsBoundsAndRevocation() public {
        vm.prank(alice);
        vm.expectRevert(LinearBondingMarket.UnauthorizedOperator.selector);
        market.setFee(40);
        market.setOperator(operator);
        vm.prank(operator);
        market.setFee(100);
        assertEq(market.feeBps(), 100);
        vm.prank(operator);
        vm.expectRevert(LinearBondingMarket.InvalidFee.selector);
        market.setFee(101);
        vm.prank(operator);
        vm.expectRevert();
        market.claimFees(operator);
        vm.prank(operator);
        vm.expectRevert();
        market.setOperator(operator);
        market.setOperator(address(0));
        vm.prank(operator);
        vm.expectRevert(LinearBondingMarket.UnauthorizedOperator.selector);
        market.setFee(40);
    }

    function testSlippageExpiryAndRecipient() public {
        (uint256 cost,) = market.quoteBuy(10 ether);
        vm.prank(alice);
        vm.expectRevert(LinearBondingMarket.Slippage.selector);
        market.buy{value: cost - 1}(10 ether, alice, block.timestamp);
        vm.warp(100);
        vm.prank(alice);
        vm.expectRevert(LinearBondingMarket.Expired.selector);
        market.buy{value: cost}(10 ether, alice, 99);
        vm.prank(alice);
        vm.expectRevert(LinearBondingMarket.InvalidRecipient.selector);
        market.buy{value: cost}(10 ether, address(market), 100);
        buy(10 ether);
        (uint256 payout,) = market.quoteSell(10 ether);
        vm.prank(alice);
        vm.expectRevert(LinearBondingMarket.Slippage.selector);
        market.sell(10 ether, payout + 1, alice, 100);
        assertBacking();
    }

    function testRejectedPayoutRollsBack() public {
        buy(10 ether);
        (uint256 payout,) = market.quoteSell(10 ether);
        RejectHype reject = new RejectHype();
        vm.prank(alice);
        vm.expectRevert(LinearBondingMarket.NativeTransferFailed.selector);
        market.sell(10 ether, payout, address(reject), block.timestamp);
        assertEq(market.sold(), 10 ether);
        assertBacking();
    }

    function testReentrancyCannotClaimTwice() public {
        buy(10 ether);
        ReenterCurve recipient = new ReenterCurve(market);
        market.claimFees(address(recipient));
        assertFalse(recipient.succeeded());
        assertBacking();
    }

    function testRejectsTransferTaxOnCreationAndTrading() public {
        CurveToken taxed = new CurveToken(18);
        taxed.mint(address(this), ALLOCATION);
        taxed.approve(address(factory), ALLOCATION);
        taxed.setTaxed(true);
        vm.expectRevert(LinearBondingMarket.UnsupportedTransfer.selector);
        factory.createMarket(address(taxed), ALLOCATION, 1 ether, 2 ether, 30, 5, 100);
        assertEq(factory.marketCount(), 1);
        buy(10 ether);
        token.setTaxed(true);
        (uint256 cost,) = market.quoteBuy(1 ether);
        vm.prank(alice);
        vm.expectRevert(LinearBondingMarket.UnsupportedTransfer.selector);
        market.buy{value: cost}(1 ether, alice, block.timestamp);
        (uint256 payout,) = market.quoteSell(1 ether);
        vm.prank(alice);
        vm.expectRevert(LinearBondingMarket.UnsupportedTransfer.selector);
        market.sell(1 ether, payout, alice, block.timestamp);
        assertBacking();
    }

    function testSixDecimalTokenAndFlatCurve() public {
        CurveToken six = new CurveToken(6);
        six.mint(address(this), 1000e6);
        six.approve(address(factory), 1000e6);
        LinearBondingMarket other =
            factory.createMarket(address(six), 1000e6, 1 ether, 1 ether, 0, 0, 0);
        (uint256 cost, uint256 fee) = other.quoteBuy(2e6);
        assertEq(cost, 2 ether);
        assertEq(fee, 0);
        vm.prank(alice);
        other.buy{value: cost}(2e6, alice, block.timestamp);
        assertEq(six.balanceOf(alice), 2e6);
        (uint256 payout,) = other.quoteSell(2e6);
        assertEq(payout, cost);
    }

    function testInvalidConfigurations() public {
        vm.expectRevert(LinearBondingMarket.InvalidConfiguration.selector);
        new LinearBondingMarket(address(token), address(this), 0, 1, 2, 30, 5, 100);
        vm.expectRevert(LinearBondingMarket.InvalidConfiguration.selector);
        new LinearBondingMarket(address(token), address(this), ALLOCATION, 2, 1, 30, 5, 100);
        vm.expectRevert(LinearBondingMarket.InvalidFee.selector);
        new LinearBondingMarket(address(token), address(this), ALLOCATION, 1, 2, 30, 31, 100);
        CurveToken bad = new CurveToken(19);
        vm.expectRevert(LinearBondingMarket.InvalidConfiguration.selector);
        new LinearBondingMarket(address(bad), address(this), ALLOCATION, 1, 2, 30, 5, 100);
    }

    function testFuzzRoundTripNeverProfits(uint96 input) public {
        uint256 amount = bound(uint256(input), 1e6, ALLOCATION);
        uint256 before = alice.balance;
        buy(amount);
        market.setFee(5);
        (uint256 payout,) = market.quoteSell(amount);
        vm.prank(alice);
        market.sell(amount, payout, alice, block.timestamp);
        assertLe(alice.balance, before);
        assertBacking();
    }

    function testFuzzSplitBuysSameBacking(uint96 first, uint96 second) public {
        uint256 a = bound(uint256(first), 1e6, ALLOCATION / 2);
        uint256 b = bound(uint256(second), 1e6, ALLOCATION / 2);
        buy(a);
        buy(b);
        assertEq(market.backing(), market.backingAt(a + b));
        assertBacking();
    }

    function testFuzzBudgetQuoteIsMaximal(uint96 input) public view {
        uint256 budget = bound(uint256(input), 1e9, 3 ether);
        (uint256 amount, uint256 cost,) = market.quoteBuyHype(budget);
        assertLe(cost, budget);
        if (amount < ALLOCATION) {
            (uint256 nextCost,) = market.quoteBuy(amount + 1);
            assertGt(nextCost, budget);
        }
    }
}

contract CurveHandler is Test {
    LinearBondingMarket public market;
    CurveToken public token;

    constructor(LinearBondingMarket m, CurveToken t) {
        market = m;
        token = t;
        token.approve(address(market), type(uint256).max);
    }
    receive() external payable {}

    function buy(uint96 input) external {
        uint256 remaining = market.allocation() - market.sold();
        if (remaining < 1e6) return;
        uint256 amount = bound(uint256(input), 1e6, remaining);
        (uint256 cost,) = market.quoteBuy(amount);
        vm.deal(address(this), address(this).balance + cost);
        market.buy{value: cost}(amount, address(this), block.timestamp);
    }

    function buyBudget(uint96 input) external {
        if (market.allocation() == market.sold()) return;
        uint256 budget = bound(uint256(input), 1e9, 1 ether);
        vm.deal(address(this), address(this).balance + budget);
        market.buyHype{value: budget}(1, address(this), block.timestamp);
    }

    function sell(uint96 input) external {
        uint256 balance = token.balanceOf(address(this));
        if (balance < 1e6) return;
        uint256 amount = bound(uint256(input), 1e6, balance);
        (uint256 payout,) = market.quoteSell(amount);
        market.sell(amount, payout, address(this), block.timestamp);
    }

    function fee(uint16 input) external {
        market.setFee(uint16(bound(input, 5, 100)));
    }

    function claim() external {
        if (market.accruedFees() > 0) market.claimFees(address(this));
    }

    function acceptOwnership() external {
        market.acceptOwnership();
    }
}

contract CurveInvariantTest is Test {
    LinearBondingMarket market;
    CurveToken token;

    function setUp() public {
        CurveFactory factory = new CurveFactory();
        token = new CurveToken(18);
        token.mint(address(this), 1000 ether);
        token.approve(address(factory), 1000 ether);
        market =
            factory.createMarket(address(token), 1000 ether, 0.001 ether, 0.003 ether, 30, 5, 100);
        CurveHandler handler = new CurveHandler(market, token);
        market.transferOwnership(address(handler));
        handler.acceptOwnership();
        bytes4[] memory selectors = new bytes4[](5);
        selectors[0] = handler.buy.selector;
        selectors[1] = handler.sell.selector;
        selectors[2] = handler.fee.selector;
        selectors[3] = handler.claim.selector;
        selectors[4] = handler.buyBudget.selector;
        targetSelector(FuzzSelector({addr: address(handler), selectors: selectors}));
        targetContract(address(handler));
    }

    function invariantBackingAndInventoryConserved() public view {
        assertEq(address(market).balance, market.backing() + market.accruedFees());
        assertEq(token.balanceOf(address(market)) + market.sold(), market.allocation());
    }
}
