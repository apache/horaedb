use influxdb_influxql_parser::string::Regex;

// copy from influxdb_iox.
/// Removes all `/` patterns that the rust regex library would reject
/// and rewrites them to their unescaped form.
///
/// For example, `\:` is rewritten to `:` as `\:` is not a valid
/// escape sequence in the `regexp` crate but is valid in golang's
/// regexp implementation.
///
/// This is done for compatibility purposes so that the regular
/// expression matching in Rust more closely follows the matching in
/// golang, used by the influx storage rpc.
///
/// See <https://github.com/rust-lang/regex/issues/501> for more details
pub fn clean_non_meta_escapes(pattern: &str) -> String {
    if pattern.is_empty() {
        return pattern.to_string();
    }

    #[derive(Debug, Copy, Clone)]
    enum SlashState {
        No,
        Single,
        Double,
    }

    let mut next_state = SlashState::No;

    let next_chars = pattern
        .chars()
        .map(Some)
        .skip(1)
        .chain(std::iter::once(None));

    // emit char based on previous
    let new_pattern: String = pattern
        .chars()
        .zip(next_chars)
        .filter_map(|(c, next_char)| {
            let cur_state = next_state;
            next_state = match (c, cur_state) {
                ('\\', SlashState::No) => SlashState::Single,
                ('\\', SlashState::Single) => SlashState::Double,
                ('\\', SlashState::Double) => SlashState::Single,
                _ => SlashState::No,
            };

            // Decide to emit `c` or not
            match (cur_state, c, next_char) {
                (SlashState::No, '\\', Some(next_char))
                | (SlashState::Double, '\\', Some(next_char))
                    if !is_valid_character_after_escape(next_char) =>
                {
                    None
                }
                _ => Some(c),
            }
        })
        .collect();

    new_pattern
}

fn is_valid_character_after_escape(c: char) -> bool {
    // same list as https://docs.rs/regex-syntax/0.6.25/src/regex_syntax/ast/parse.rs.html#1445-1538
    match c {
        '0'..='7' => true,
        '8'..='9' => true,
        'x' | 'u' | 'U' => true,
        'p' | 'P' => true,
        'd' | 's' | 'w' | 'D' | 'S' | 'W' => true,
        _ => regex_syntax::is_meta_character(c),
    }
}

/// Sanitize an InfluxQL regular expression and create a compiled
/// [`regex::Regex`].
pub fn parse_regex(re: &Regex) -> std::result::Result<regex::Regex, regex::Error> {
    let pattern = clean_non_meta_escapes(re.as_str());
    regex::Regex::new(&pattern)
}
