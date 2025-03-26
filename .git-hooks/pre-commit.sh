#!/bin/sh
#
# GitHooks.PreCommit: Main pre-commit hook
# This hook performs the following checks in order:
# 1. Verifies non-ASCII filenames
# 2. Checks for whitespace errors
# 3. Strips output from Jupyter notebooks

# GitHooks.NonAsciiValidator: Check for non-ASCII filenames
GitHooks_validate_filenames() {
    echo "GitHooks.NonAsciiValidator: Checking for non-ASCII filenames..."

    # Declare first, then assign to avoid masking return values
    allownonascii=""
    allownonascii=$(git config --type=bool hooks.allownonascii)

    # Non-ASCII filename check
    if [ "$allownonascii" != "true" ] &&
        [ "$(git diff-index --cached --name-only --diff-filter=A -z "$GitHooks_against" |
          LC_ALL=C tr -d '[ -~]\0' | wc -c)" != "0" ]
    then
        cat <<EOF
Error: Attempt to add a non-ASCII file name.

This can cause problems if you want to work with people on other platforms.

To be portable it is advisable to rename the file.

If you know what you are doing you can disable this check using:

  git config hooks.allownonascii true
EOF
        return 1
    fi

    echo "GitHooks.NonAsciiValidator: No non-ASCII filenames found"
    return 0
}

# GitHooks.WhitespaceValidator: Check for whitespace errors
GitHooks_check_whitespace() {
    echo "GitHooks.WhitespaceValidator: Checking for whitespace errors..."

    # Run Git's built-in whitespace check
    if ! git diff-index --check --cached "$GitHooks_against" --; then
        echo "GitHooks.WhitespaceValidator: Whitespace errors found"
        return 1
    fi

    echo "GitHooks.WhitespaceValidator: No whitespace errors found"
    return 0
}



# GitHooks.NotebookProcessor: Clean Jupyter notebook outputs
# First backup the original notebook so that we can restore the output after commit to avoid having to re-run the notebook

GitHooks_process_notebooks() {
    echo "GitHooks.NotebookProcessor: Scanning for Jupyter notebooks..."

    # Find all staged .ipynb files (POSIX-compliant approach)
    GitHooks_notebooks=""
    GitHooks_notebooks=$(git diff --cached --name-only --diff-filter=ACMR | grep "\.ipynb$" || true)

    if [ -n "$GitHooks_notebooks" ]; then
        # Check if nbstripout is installed
        if ! command -v nbstripout >/dev/null 2>&1; then
            echo "GitHooks.NotebookProcessor: Error - nbstripout not found"
            echo "Please install with: pip install nbstripout"
            return 1
        fi

        echo "GitHooks.NotebookProcessor: Cleaning notebook outputs from:"
        echo "$GitHooks_notebooks"

        # Process each notebook individually for better cross-platform compatibility
        for notebook in $GitHooks_notebooks; do
            nbstripout "$notebook"
            git add "$notebook"
        done

        echo "GitHooks.NotebookProcessor: Notebook cleaning complete"
    else
        echo "GitHooks.NotebookProcessor: No notebooks to process"
    fi

    return 0
}

# GitHooks.Main: Main execution flow
GitHooks_main() {
    # Redirect output to stderr
    exec 1>&2

    # Determine the comparison base
    GitHooks_against=""
    if git rev-parse --verify HEAD >/dev/null 2>&1; then
        GitHooks_against=HEAD
    else
        # Initial commit: diff against an empty tree object
        GitHooks_against=$(git hash-object -t tree /dev/null)
    fi

    # Run each check in sequence - consistent ordering
    GitHooks_validate_filenames || return 1
    # GitHooks_process_notebooks || return 1y
    GitHooks_check_whitespace || return 1

    echo "GitHooks.PreCommit: All checks passed successfully"
    return 0
}

# Execute main function and exit with appropriate status
GitHooks_main || {
    echo "GitHooks.PreCommit: Pre-commit checks failed"
    exit 1
}

# All checks passed
exit 0