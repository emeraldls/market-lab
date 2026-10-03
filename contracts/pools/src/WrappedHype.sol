// SPDX-License-Identifier: GPL-3.0-only
pragma solidity ^0.8.24;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import {Address} from "@openzeppelin/contracts/utils/Address.sol";

/// @notice Native HYPE held 1:1 against ERC-20 balances. No owner or privileged minting.
contract WrappedHype is ERC20, ReentrancyGuard {
    constructor() ERC20("Wrapped HYPE", "WHYPE") {}

    receive() external payable {
        deposit();
    }

    function deposit() public payable {
        _mint(msg.sender, msg.value);
    }

    function withdraw(uint256 amount) external nonReentrant {
        _burn(msg.sender, amount);
        Address.sendValue(payable(msg.sender), amount);
    }
}
