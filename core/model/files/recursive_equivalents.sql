CREATE OR REPLACE FUNCTION fn_recursive_equivalents(parent INT, recursion_depth INT, strength_threshold DOUBLE PRECISION, cutoff INT DEFAULT null)
RETURNS TABLE
        (
        recursive_equivalent INT
        )
AS
$$
        WITH RECURSIVE
                find_equivs(n, strength, input_id, output_id) AS
                (
                SELECT 1, 1::DOUBLE PRECISION, $1 as input_id, $1 as output_id, 0::BIGINT as r
                UNION
                SELECT fe.n + 1, fe.strength * e.strength, e.input_id, e.output_id, row_number() over (order by null) as r
                FROM equivalents e, find_equivs fe
                WHERE fe.n <= $2
                        AND fe.strength * e.strength > $3
                        AND (
                        e.input_id = fe.input_id
                        OR e.input_id = fe.output_id
                        OR e.output_id = fe.input_id
                        OR e.output_id = fe.output_id
                        )
			AND e.enabled = true
			AND ($4 is null or r < $4)
                )
        SELECT input_id as id
        FROM find_equivs
        UNION
        SELECT output_id as id
        FROM find_equivs
$$
LANGUAGE 'sql'
VOLATILE;
