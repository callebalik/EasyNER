- It's a flask app.
- Keep changes within server functionallity, do not modify core database scripts without first explaining your plan and asking for confirmation.
- Never use style blocks within html, use or augment proper scss partials in "scripts/database/static/partials" to keep theming managable. Remeber to do proper import via main scripts/database/static/styles.scss
- As far as possible create modular reusable Typescript in scripts/"database/static/js" except for small blocks of very highly coulpled functionality to one html.
- Refactor, reuse and extend when possible, before creating new styles, scripts and other functionality. It should be managable and modular.



