CREATE OR REPLACE FUNCTION fn_recursive_equivalents(parent INT, recursion_depth INT, strength_threshold NUMERIC)
RETURNS TABLE
        (
        recursive_equivalent INT
        )
AS
$$
        WITH RECURSIVE
                find_equivs(n, strength, input_id, output_id) AS
                (
                SELECT 1, (1 - (1 - $3)), $1 as input_id, $1 as output_id
                UNION
                SELECT fe.n + 1, (1 - (1 - $3)^(fe.n + 1)), e.input_id, e.output_id
                FROM equivalents e, find_equivs fe
                WHERE fe.n <= $2
                        AND e.strength > fe.strength
                        AND (
                        e.input_id = fe.input_id
                        OR e.input_id = fe.output_id
                        OR e.output_id = fe.input_id
                        OR e.output_id = fe.output_id
                        )
                )
        SELECT input_id as id
        FROM find_equivs
        UNION
        SELECT output_id as id
        FROM find_equivs
$$
LANGUAGE 'sql'
VOLATILE;