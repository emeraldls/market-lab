// SPDX-License-Identifier: GPL-3.0-only
pragma solidity ^0.8.24;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";

/// @notice Fixed-supply token for trying the Elysium pool workflow.
contract TestToken is ERC20 {
    constructor(string memory name_, string memory symbol_, uint256 supply, address recipient) ERC20(name_, symbol_) {
        require(bytes(name_).length > 0 && bytes(name_).length <= 64, "Invalid name");
        require(bytes(symbol_).length > 0 && bytes(symbol_).length <= 12, "Invalid symbol");
        require(supply > 0, "Supply must be positive");
        _mint(recipient, supply);
    }
}
