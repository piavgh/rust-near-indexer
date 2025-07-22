use std::str::FromStr;

/// Utility functions for handling decimal amounts as u128 values.
use rust_decimal::Decimal;

/// Convert a rust_decimal::Decimal to u128 raw value for ClickHouse.
/// Since the decimal values are already in smallest units (e.g., wei), we just need the raw integer value.
pub fn decimal_to_u128(decimal: Decimal) -> u128 {
    // For crypto amounts in smallest units (wei), the decimal should represent whole units only
    // Convert to string, ensure it's a whole number (no decimal point for wei values)
    let decimal_str = decimal.to_string();

    // Check if it contains a decimal point (shouldn't happen for wei values)
    if decimal_str.contains('.') {
        eprintln!(
            "Warning: Decimal '{}' contains fractional part, but wei values should be whole numbers. Converting to truncated integer.",
            decimal
        );
        // Take only the integer part
        let integer_part = decimal_str.split('.').next().unwrap_or("0");
        integer_part.parse::<u128>().unwrap_or(0)
    } else {
        decimal_str.parse::<u128>().unwrap_or_else(|_| {
            eprintln!(
                "Warning: Failed to convert decimal '{}' to u128, using 0",
                decimal
            );
            0
        })
    }
}

/// Convert a u128 raw value back to rust_decimal::Decimal.
/// Since the values are already in smallest units, we just parse the u128 directly.
pub fn u128_to_decimal(raw_value: u128) -> Decimal {
    Decimal::from_str(&raw_value.to_string()).unwrap_or(Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal_conversions() {
        // Test decimal to u128 (values already in wei/smallest units)
        let decimal_1000 = Decimal::from_str("1000").unwrap();
        assert_eq!(decimal_to_u128(decimal_1000), 1000);

        let decimal_large = Decimal::from_str("1000000000000000000").unwrap(); // 1 ETH in wei
        assert_eq!(decimal_to_u128(decimal_large), 1000000000000000000);

        // Test u128 to decimal
        assert_eq!(u128_to_decimal(1000), Decimal::from_str("1000").unwrap());
        assert_eq!(
            u128_to_decimal(1000000000000000000),
            Decimal::from_str("1000000000000000000").unwrap()
        );

        // Test very large crypto amounts (no overflow)
        // This represents about 1 billion ETH in wei (realistic large amount)
        let very_large = Decimal::from_str("1000000000000000000000000000").unwrap(); // 10^27 wei
        let converted = decimal_to_u128(very_large);
        assert_eq!(converted, 1000000000000000000000000000u128);
        assert_eq!(u128_to_decimal(converted), very_large);
    }
}
