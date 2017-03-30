************
GAVL Grammar
************

::

    program      = statement { statement } ;
    statement    = [ <variable> "=" ] expr ;
    expression   = selection { "|" bool_expr } ;
    selection    = arith { "," arith } ;
    arith        = arith_term  { ("+" | "-") arith_term } ;
    arith_term   = arith_factor  { ("*" | "/") arith_factor } ;
    arith_factor = constant | variable | "("  expression  ")" ;
    bool_expr    = bool_and "or" bool_and ;
    bool_and     = bool_term "and" bool_term ;
    bool_term    = bool_not ("<" | ">" | "<=" | ">=" | "==") bool_not ;
    bool_not     = "not" bool_factor | bool_factor ;
    bool_factor  = bool_const | variable | "("  bool_expr  ")" ;
    bool_const   = "True" | "False"
    variable     = (alphas | "_") { "." (alphas | "_") } ;
    constant     = [ sign ] { digits } "." { digits } | [ sign ] digits { digits } ;
    lowers       = 'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' |
                   'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' |
                   'u' | 'v' | 'w' | 'x' | 'y' | 'z' ;
    uppers       = 'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' |
                   'K' | 'L' | 'M' | 'N' | 'O' | 'P' | 'Q' | 'R' | 'S' | 'T' |
                   'U' | 'V' | 'W' | 'X' | 'Y' | 'Z' ;
    alphas       = lowers | uppers ;
    digits       = '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' | '0' ;
    alphanumeric = alphas | digits ;
    sign         = "+" | "-"


