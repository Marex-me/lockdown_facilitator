SELECT
	d.name
FROM
	drinks d
		JOIN
	measures m
		ON d.id = m.drink_id
		AND m.measure like '15%g'
		JOIN
	ingredients i
		ON m.ingredient_id = i.id
		AND i.name = 'Sambuka';
