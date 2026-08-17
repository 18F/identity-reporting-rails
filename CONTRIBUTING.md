## Welcome!

We’re so glad you’re thinking about contributing to a Technology Transformation Services (TTS) open source project! If you’re unsure about anything, just ask — or submit your pull request anyway. The worst that can happen is we’ll politely ask you to change something. We appreciate all friendly contributions.

TTS is committed to building a safe, welcoming, harassment-free culture for everyone. We expect everyone on the TTS team and everyone within TTS spaces, including contributors to our projects, to follow the [TTS Code of Conduct](https://github.com/18F/code-of-conduct/blob/master/code-of-conduct.md).

We encourage you to read this project’s CONTRIBUTING policy (you are here), its [LICENSE](LICENSE.md), and its [README](README.md). When you are ready to make a pull request, read our [pull request process](https://handbook.login.gov/articles/pull-request-review.html), which is a part of [the Login.gov Handbook](https://handbook.login.gov/).

If you have any questions or want to read more, check out the [18F Open Source Policy GitHub repository]( https://github.com/18f/open-source-policy), or [send us an email](mailto:18f@gsa.gov).


## Pull request guidelines

Below are rules we strive to follow to achieve maintainable and consistent code.

### Commit message style guide

We follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/),
using **only the header line** — no body and no footer.

```
<type>[(optional scope)]: <description>
```

Example:

```
feat(fraudops): flatten event fields into typed columns
```

Rules for the header:

- **`<type>`** — one of the standard Conventional Commits types:
  `feat`, `fix`, `build`, `chore`, `ci`, `docs`, `style`, `refactor`, `perf`,
  `test`, `revert`. `feat` and `fix` are the two the spec requires; the rest are
  the standard set. Do not invent new types.
- **`(scope)`** — optional, in parentheses, a short noun for the area of the
  codebase (free-form). Examples: `(fraudops)`, `(db)`, `(ci)`, `(masking)`.
  Omit it if it adds nothing.
- **`: ` (colon + single space)** separates the prefix from the description.
- **description** — imperative mood ("add", not "added"/"adds"), lowercase
  first letter, no trailing period, and short (aim for ≤ 72 characters).

More examples:

```
fix(fraudops): preserve success=false instead of coercing to NULL
refactor: derive event columns from a single config map
test(fraudops): cover success false-vs-nil handling
docs: document the frd_events backfill task
chore: refresh Brakeman ignore fingerprint
```

#### Breaking changes

Because we keep to the header only, indicate a breaking change with a `!`
after the type/scope (rather than a `BREAKING CHANGE:` footer):

```
feat(fraudops)!: rename frd_events columns, breaking downstream dbt models
```

### Branch naming

Name branches `<type>/<issue-iid>-<short-kebab-description>`, using the same
type vocabulary as commits and the GitLab issue IID (the numeric issue ID, e.g.
`1565`):

```
feat/1565-extract-event-columns
fix/1517-redshift-sync-secret-fetch
```

- When there is no associated issue, omit the IID: `<type>/<short-kebab-description>`
  (e.g. `docs/update-contributing-conventions`).
- Use lowercase kebab-case for the description.
- `stages/*` is reserved for deploy/staging branches — do not use it for feature
  work.

### Merge request title and description

- **Title:** use the same Conventional Commits header format as commits (e.g.
  `feat(fraudops): flatten event fields into typed columns`). Merge requests are
  squash-merged and the title becomes the commit subject on `main`, so it must
  follow the convention.
- **Description:** fill out the default MR template
  (`.gitlab/merge_request_templates/Default.md`) — link the issue in the
  **Ticket** section, explain the change in **Summary**, and record how it was
  checked in **Verification**. Because commit headers carry no body, the MR
  description is where the "why" and context live. Remove any template section
  or checklist item that does not apply — don't leave placeholders or mark them
  N/A.

### Additional notes on pull requests and code reviews

Please follow our [Code Review][review] guidelines.
[Glen Sanford's thoughts on code reviews][thoughts] are also well worth
reading.

[review]: https://engineering.18f.gov/code-review/
[thoughts]: http://glen.nu/ramblings/oncodereview.php

- Keep pull requests as small as possible, and focused on a single topic
- Once a pull request is good to go, the person who opened it squashes related
commits together, merges it, then deletes the branch.

Everyone is encouraged to participate in code review. To solicit feedback from specific people,
consider adding individuals or groups as requested reviewers on your pull request. Most internal
product teams have a team handle which can be used to notify everyone on that team, or you can
request reviews from one of the available interest group teams:

To request to join any of these teams, you can contact any existing member and ask to be added.

## Public domain

This project is in the public domain within the United States, and
copyright and related rights in the work worldwide are waived through
the [CC0 1.0 Universal public domain dedication][CC0].

All contributions to this project will be released under the CC0
dedication. By submitting a pull request, you are agreeing to comply
with this waiver of copyright interest.

[CC0]: https://creativecommons.org/publicdomain/zero/1.0/
[FormResponse]: https://github.com/18F/identity-idp/blob/master/app/services/form_response.rb
[EmailConfirmationTokenValidator]: https://github.com/18F/identity-idp/blob/master/app/services/email_confirmation_token_validator.rb
[PasswordForm]: https://github.com/18F/identity-idp/blob/master/app/forms/password_form.rb
[cache poisoning attacks]: https://github.com/rails/rails/issues/29893
