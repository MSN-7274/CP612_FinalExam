SELECT COUNT(*) AS cs_students_in_canada
FROM (
        SELECT StudentID, Name, Major,
            CASE
                WHEN LOWER(University) IN ('wlu', 'wilfrid laurier university') THEN 'Wilfrid Laurier University'
                WHEN LOWER(University) IN ('yorku', 'york university') THEN 'York University'
                WHEN LOWER(University)    = 'uofwaterloo' THEN 'University of Waterloo'
                WHEN LOWER(University)    = 'standford university' THEN 'Stanford University'
                WHEN LOWER(University) IN ('nus', 'national university of singapore') THEN 'National University of Singapore'
                ELSE University
            END AS UnivStdName
        FROM Student
        WHERE Major = 'Computer Science'
     ) AS S
JOIN   University U
       ON S.UnivStdName = U.UniversityName
WHERE  U.Country = 'Canada';
