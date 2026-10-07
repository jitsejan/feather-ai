# Evidence Dashboard Setup

## Prerequisites

- Node.js and npm installed
- Access to MotherDuck with a valid token

## Setup Instructions

### 1. Install Dependencies

```bash
npm install
```

### 2. Configure MotherDuck Connection

Create a `connection.options.yaml` file from the example:

```bash
cd sources/motherduck
cp connection.options.yaml.example connection.options.yaml
```

### 3. Get Your MotherDuck Token

```bash
duckdb "md:"
```

Then in the DuckDB shell:

```sql
PRAGMA PRINT_MD_TOKEN;
```

Copy the token value.

### 4. Update the Connection File

Edit `sources/motherduck/connection.options.yaml` and replace `YOUR_MOTHERDUCK_TOKEN_HERE` with your actual token.

**Important:** This file is in `.gitignore` and will NOT be committed to git.

### 5. Run the Dashboard

```bash
npm run dev
```

Or from the project root:

```bash
make dashboard
```

The dashboard will be available at http://localhost:3000/

## Refreshing Data

To refresh the data from MotherDuck without restarting:

```bash
npm run sources
```

## Available Pages

- **Home** (`/`) - Executive overview with KPIs and summary charts
- **Sprint Analytics** (`/sprints`) - Deep dive into sprint performance (DT team only)
- **Team Performance** (`/team`) - Individual and team metrics
- **Ticket Analysis** (`/tickets`) - Ticket aging insights
- **User Insights** (`/null_assignees`) - Quality checks for unassigned issues

## Notes

- The dashboard filters to show only **DT team sprints**
- Completed tickets (Done, Won't Do, Closed, Cancelled) are excluded from aging analysis
- Data is cached; run `npm run sources` to refresh
