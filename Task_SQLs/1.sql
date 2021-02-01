SELECT
	d.name
FROM
	drinks d
        JOIN
	(SELECT
        m.drink_id AS drink_id,
        lower(group_concat(i.name)) AS ingredient_mix
    FROM
        measures m
            JOIN
        ingredients i
            ON m.ingredient_id = i.id
            AND (lower(i.name) like '%lemon%'
            OR lower(i.name) like '%whiskey%')
    GROUP BY
        drink_id
	) fi
		ON fi.drink_id = d.id
		AND fi.ingredient_mix like '%lemon%'
		AND fi.ingredient_mix like '%whiskey%'
WHERE
	d.alcoholic = 1;
