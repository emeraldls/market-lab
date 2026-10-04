// SPDX-License-Identifier: GPL-3.0-only
pragma solidity ^0.8.24;

import {Test} from "forge-std/Test.sol";
import {TestToken} from "../src/TestToken.sol";

contract TestTokenTest is Test {
    function testSupplyAndTransfer() public {
        TestToken token = new TestToken("Sample Token", "SAMPLE", 1_000_000 ether, address(this));
        assertEq(token.name(), "Sample Token");
        assertEq(token.symbol(), "SAMPLE");
        assertEq(token.decimals(), 18);
        assertEq(token.totalSupply(), 1_000_000 ether);
        assertEq(token.balanceOf(address(this)), token.totalSupply());
        token.transfer(address(123), 200_000 ether);
        assertEq(token.balanceOf(address(123)), 200_000 ether);
        assertEq(token.totalSupply(), 1_000_000 ether);
        (bool minted,) = address(token).call(abi.encodeWithSignature("mint(address,uint256)", address(this), 1 ether));
        assertFalse(minted);
    }

    function testInvalidInputs() public {
        vm.expectRevert();
        new TestToken("", "ABC", 1, address(this));
        vm.expectRevert();
        new TestToken("ABC", "", 1, address(this));
        vm.expectRevert();
        new TestToken("ABC", "ABC", 0, address(this));
        vm.expectRevert();
        new TestToken("ABC", "ABC", 1, address(0));
    }
}
