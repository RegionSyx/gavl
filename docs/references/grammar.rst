************
GAVL Grammar
************

::

    program      = statement { statement } ;
    statement    = [ <variable> "=" ] expr ;
    expression   = arith { "|" bool_expr } ;
    arith        = term  { ("+" | "-") term } ;
    term         = factor  { ("*" | "/") factor } ;
    factor       = constant | variable | "("  expression  ")" ;
    bool_expr    = and_expr "or" and_expr ;
    and_expr     = bool_term "and" bool_term ;
    bool_term    = bool_factor ("<" | ">" | "<=" | ">=" | "==") bool_factor ;
    bool_factor  = bool_const | variable | "("  bool_expr  ")" ;
    bool_const   = "True" | "False"
    variable     = alphas { "." alphas } ;
    constant     = numbers { numbers } ;
    lowers       = 'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' |
                   'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' |
                   'u' | 'v' | 'w' | 'x' | 'y' | 'z' ;
    uppers       = 'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' |
                   'K' | 'L' | 'M' | 'N' | 'O' | 'P' | 'Q' | 'R' | 'S' | 'T' |
                   'U' | 'V' | 'W' | 'X' | 'Y' | 'Z' ;
    alphas       = lowers | uppers ;
    numbers      = '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' | '0' ;
    alphanumeric = alphas | numbers ;


