-- Over 70K equivalents in the Wrangler db point an identifier back to itself.
-- This SQL query will delete those rows.
delete from equivalents
where id in (
    select e.id from equivalents e
    join identifiers inputs on e.input_id = inputs.id
    join identifiers outputs on e.output_id = outputs.id
    where inputs.id = outputs.id
);
