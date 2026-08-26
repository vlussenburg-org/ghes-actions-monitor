# Security policy

## Scope

This project is provided as open-source software and is not an official
GitHub product. Review the deployment, authentication, credential, and network
configuration before using it with a production GitHub Enterprise Server or
GitHub organization.

The monitor can read workflow and runner data and can cancel workflow runs.
Treat its API, database, logs, environment variables, and backups as sensitive.
Use least-privilege GitHub App credentials where possible, enable webhook
signatures, enable the built-in dashboard/API authentication or put the
service behind an authenticating proxy, and restrict network access.

## Reporting a vulnerability

Do not report security vulnerabilities in a public issue. Use the repository's
private security advisory or vulnerability-reporting mechanism when available.
If that mechanism is unavailable, contact the repository maintainers privately
with a description, impact, reproduction steps, and any suggested mitigation.

Please do not include credentials, webhook secrets, customer data, or other
sensitive information in a report.
