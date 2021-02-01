SELECT
	d.name
FROM
	drinks d
		JOIN
	(
		SELECT
			drink_id,
			MAX(ingredients)
		FROM
			(
				SELECT
					drink_id,
					count(*) AS ingredients
				FROM
					measures
				GROUP BY
					drink_id
			)
	) mi
		ON d.id = mi.drink_id;
