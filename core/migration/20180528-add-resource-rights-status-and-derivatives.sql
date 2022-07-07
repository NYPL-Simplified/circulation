DO $$
  BEGIN
    BEGIN
      ALTER TABLE resources ADD COLUMN rights_status_id integer;
      ALTER TABLE resources ADD CONSTRAINT resources_rightsstatus_id_fkey FOREIGN KEY (rights_status_id) REFERENCES rightsstatus(id);
    EXCEPTION
      WHEN duplicate_column THEN RAISE NOTICE 'column resources.rights_status_id already exists, not creating it.';
    END;

    BEGIN
      ALTER TABLE resources ADD COLUMN rights_explanation varchar;
    EXCEPTION
      WHEN duplicate_column THEN RAISE NOTICE 'column resources.rights_explanation already exists, not creating it.';
    END;
  END;
$$;
