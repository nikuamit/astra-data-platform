# CI/CD Setup

The workflow file  requires a GitHub token with  scope to push via API.

## To activate CI/CD — run once in your terminal:

```bash
# 1. mkdir .github/workflows
# 2. Copy ci.yaml content below into .github/workflows/ci.yaml
# 3. git add .github/workflows/ci.yaml && git commit -m "feat: add CI/CD" && git push
```

## ci.yaml content:

```yaml
name: CI

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    name: Unit Tests
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Set up Java (PySpark needs JVM)
        uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "17"

      - name: Install dependencies
        run: |
          pip install --upgrade pip
          pip install -r requirements.txt

      - name: Lint
        run: |
          pip install ruff
          ruff check .

      - name: Run tests
        run: |
          pytest tests/unit/ -v --tb=short --junitxml=reports/junit.xml

      - name: Upload test report
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: test-report
          path: reports/junit.xml

  release:
    name: Tag & Release
    needs: test
    if: github.ref == 'refs/heads/main' && github.event_name == 'push'
    runs-on: ubuntu-latest

    permissions:
      contents: write

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Bump version and push tag
        uses: mathieudutour/github-tag-action@v6.1
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          default_bump: minor

      - name: Create GitHub Release
        uses: softprops/action-gh-release@v1
        with:
          generate_release_notes: true

```

Once pushed, every merge to main will:
1. Run ruff lint
2. Run pytest tests/unit/
3. Auto-tag a semver release (v0.1.0, v0.2.0 etc.)
