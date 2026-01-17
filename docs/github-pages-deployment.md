# GitHub Pages Deployment Structure

## Overview

This project uses GitHub Pages with a custom deployment structure where ALL branches (including main) are deployed to subdirectories, with a landing page at the root that provides easy navigation to all deployments.

## Directory Structure

```
/ (root)
├── index.html                 # Landing page with deployment cards
├── deployments.json           # Manifest of all active deployments
├── main/                      # Main branch deployment (primary)
│   └── [app files]
├── develop/                   # Develop branch deployment
│   └── [app files]
├── claude-feature-name/       # Feature branch deployments
│   └── [app files]
└── ...
```

## URLs

- **Landing Page**: `https://jnyinglis.github.io/MetricForge/`
- **Main Branch**: `https://jnyinglis.github.io/MetricForge/main/`
- **Develop Branch**: `https://jnyinglis.github.io/MetricForge/develop/`
- **Feature Branches**: `https://jnyinglis.github.io/MetricForge/[sanitized-branch-name]/`

Branch names with `/` are sanitized to `-` for URLs (e.g., `claude/auth` becomes `claude-auth`).

## Landing Page Features

The landing page at the root provides:
- Card-based layout matching the app's theme (cyan accent, DM Sans font)
- Dark/light theme toggle (persisted to localStorage)
- Automatic discovery of all deployments via `deployments.json`
- Metadata for each deployment:
  - Branch name
  - Deployment path
  - Commit SHA and message
  - Deployment timestamp
  - Primary badge for main branch
- Direct links to each deployment

## Workflows

### `deploy-main.yml`

Triggers on pushes to `main` branch.

**Process**:
1. Fetches existing gh-pages content to preserve other deployments
2. Builds main branch with base path `/MetricForge/main/`
3. Copies build to `/main/` subdirectory
4. Updates `deployments.json` with main deployment metadata
5. Copies landing page to root
6. Deploys all content to GitHub Pages

### `deploy-develop.yml`

Triggers on pushes to `develop`, `codex/**`, `claude/**`, and `poc/**` branches.

**Process**:
1. Fetches existing gh-pages content to preserve other deployments
2. Sanitizes branch name for directory (replaces `/` with `-`)
3. Builds preview branch with base path `/MetricForge/[branch-name]/`
4. Copies build to `/[branch-name]/` subdirectory
5. Updates `deployments.json` with preview deployment metadata
6. Copies landing page to root
7. Deploys all content to GitHub Pages

## Deployments Manifest

The `deployments.json` file contains metadata about all active deployments:

```json
{
  "deployments": [
    {
      "branch": "main",
      "path": "/main/",
      "commitSha": "abc123...",
      "commitMessage": "Update feature X",
      "deployedAt": "2026-01-17T15:30:00Z",
      "isPrimary": true
    },
    {
      "branch": "develop",
      "path": "/develop/",
      "commitSha": "def456...",
      "commitMessage": "Add feature Y",
      "deployedAt": "2026-01-17T14:20:00Z",
      "isPrimary": false
    }
  ]
}
```

Each workflow automatically:
- Reads existing manifest
- Updates or adds its deployment entry
- Removes old entry for the same branch
- Sorts deployments by timestamp (newest first)
- Writes updated manifest

## Key Implementation Details

### Preserving Deployments

Each workflow fetches the existing gh-pages branch content before deploying. This ensures that:
- Deploying main doesn't remove feature branch deployments
- Deploying a feature branch doesn't remove other branches
- The landing page and manifest are always present

### Base Path Configuration

Vite builds use the `--base` flag to ensure assets load correctly from subdirectories:
- Main: `--base=/MetricForge/main/`
- Preview: `--base=/MetricForge/[branch-name]/`

### Concurrency Control

The workflows use:
```yaml
concurrency:
  group: 'pages'
  cancel-in-progress: true
```

This ensures only one deployment runs at a time, preventing race conditions when updating the manifest.

## Development Workflow

1. **Work on a feature branch** (e.g., `claude/new-feature`)
2. **Push changes** - workflow automatically deploys to `/claude-new-feature/`
3. **View deployment** at landing page or direct URL
4. **Merge to main** - workflow deploys to `/main/` (primary)

## Migration Notes

**Previous Structure**:
- Main was at root: `https://jnyinglis.github.io/MetricForge/`
- Feature branches in subdirectories

**New Structure**:
- Landing page at root
- Main in `/main/` subdirectory
- All branches in subdirectories

This change ensures:
- Consistent structure for all deployments
- Easy navigation between branches
- Metadata about all active deployments
- No confusion about which deployment is at root

## Files

- `/pages/index.html` - Landing page source
- `/.github/workflows/deploy-main.yml` - Main branch deployment
- `/.github/workflows/deploy-develop.yml` - Preview branch deployment
- `/docs/deployments-schema.md` - Detailed manifest schema
