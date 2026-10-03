// SPDX-License-Identifier: GPL-3.0-only
pragma solidity ^0.8.24;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";

/// @dev Demo-only faucet token. Not USDC, collateral, or a production stablecoin.
contract DemoToken is ERC20 {
    constructor() ERC20("MarketLab Demo Dollar", "DEMO") {}

    function decimals() public pure override returns (uint8) {
        return 6;
    }

    function mint(address recipient, uint256 amount) external {
        _mint(recipient, amount);
    }
}
