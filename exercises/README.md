# Weekly exercises

One Markdown exercise per week, generated automatically every Monday by
`exercise_generator.py` (via `.github/workflows/exercises.yml`) from the most
recent complete Monday–Sunday week of UMPD data. Each file is named for the
Monday of the week it covers (`YYYY-MM-DD.md`) and pins its data URLs to the
commit it was generated from, so student answers in R are checkable against
the printed numbers.

These render on the site at `/exercises/`. To regenerate one by hand:

    python exercise_generator.py --date 2026-07-13 --force
