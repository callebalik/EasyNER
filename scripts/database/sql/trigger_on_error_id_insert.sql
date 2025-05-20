-- Trigger to handle cleanup when an error_id is added to a named entity
CREATE TRIGGER IF NOT EXISTS trg_ne_error_id_cleanup
AFTER UPDATE OF ERROR_ID ON NE
WHEN NEW.ERROR_ID IS NOT NULL
AND OLD.ERROR_ID IS NULL
AND OLD.NE_NORM_ID IS NOT NULL
BEGIN
    -- 1. Delete entries from DIS_PNM that reference the updated entity
    DELETE FROM DIS_PNM
    WHERE E1_ID = NEW.NE_ID OR E2_ID = NEW.NE_ID;

    -- 2. Delete entries from DIS_PNM_AGGR that reference the normalized entity
    DELETE FROM DIS_PNM_AGGR
    WHERE E1_NORM_ID = OLD.NE_NORM_ID OR E2_NORM_ID = OLD.NE_NORM_ID;

    -- 3. Update ALL NE records (including the current one) to remove references to this normalized entity
    UPDATE NE
    SET NE_NORM_ID = NULL
    WHERE NE_NORM_ID = OLD.NE_NORM_ID;

    -- 4. Remove the normalized entity from NE_AGGR
    DELETE FROM NE_AGGR
    WHERE NE_NORM_ID = OLD.NE_NORM_ID;
END;