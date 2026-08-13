# Contributing

Thanks for considering a contribution to the data processing infrastructure.

## Getting Started

```bash
npm install
npm run build   # TypeScript compilation
npm test        # Run Jest tests
npx cdk synth   # Synthesize CloudFormation template
cd consumer && python -m pytest tests  # Run Python consumer tests
```

## CI Pipeline

Every push and pull request runs:

1. `npm ci` — clean dependency install
2. `npm run build` — TypeScript compilation
3. `npm test` — Jest test suite
4. `npx cdk synth` — CDK synthesis
5. `opa eval` — OPA policy checks against synthesized CloudFormation

Make sure all five steps pass before requesting review.

CI retains the synthesized template and OPA output for seven days, including
on failed runs. Download the `infrastructure-validation-*` artifact from the
workflow run when investigating a validation failure.

The security workflow reviews dependency changes on pull requests and runs
CodeQL analysis weekly for the TypeScript and Python codebases.

## Policy Checks

OPA (Open Policy Agent) policies live in `policy/`. They are evaluated against
the synthesized CloudFormation template to enforce security and operational
guardrails. When adding new infrastructure, make sure existing policies pass
and consider adding new ones for any new compliance requirements.

## Pull Request Process

1. Keep changes focused. One PR = one concern.
2. Update tests when you add or modify infrastructure.
3. Update the README if you change configuration surfaces or architecture.
4. Make sure the CI pipeline is green.
5. Request review from a maintainer.

## Design Principles

- **Production orientation first** — every resource should reflect real-world
  security and operational practices.
- **Configurable over hardcoded** — use CDK context or environment variables.
- **Least privilege** — IAM roles, security groups, and bucket policies should
  grant the minimum access needed.
- **Tested infrastructure** — both unit tests and policy-as-code checks.
