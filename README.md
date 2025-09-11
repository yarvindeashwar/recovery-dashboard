# Recovery MIS Dashboard

A Streamlit dashboard for recovery operations intelligence using BigQuery data.

## 🚀 Deploy to Streamlit Cloud

Follow these steps to deploy your dashboard to Streamlit Cloud (free hosting):

### Step 1: Create a GitHub Repository

1. Go to [GitHub](https://github.com) and create a new repository
2. Name it something like `recovery-dashboard`
3. Make it public (required for free Streamlit Cloud hosting)

### Step 2: Push Code to GitHub

```bash
# Add your GitHub repository as remote
git remote add origin https://github.com/YOUR_USERNAME/recovery-dashboard.git

# Push your code
git branch -M main
git push -u origin main
```

### Step 3: Deploy on Streamlit Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Sign in with your GitHub account
3. Click "New app"
4. Select your repository and branch (main)
5. Set the main file path: `recovery_dashboard.py`
6. Click "Advanced settings"

### Step 4: Add Secrets in Streamlit Cloud

In the Advanced settings, add your secrets:

```toml
[gcp_credentials]
type = "authorized_user"
client_id = "YOUR_CLIENT_ID"
client_secret = "YOUR_CLIENT_SECRET"
refresh_token = "YOUR_REFRESH_TOKEN"
universe_domain = "googleapis.com"
```

**Important**: Copy the exact values from your `.streamlit/secrets.toml` file (which is gitignored and won't be uploaded to GitHub).

### Step 5: Deploy

Click "Deploy" and wait for your app to build and start. It will be available at:
`https://YOUR_APP_NAME.streamlit.app`

## 📁 Files Structure

- `recovery_dashboard.py` - Main dashboard application
- `requirements.txt` - Python dependencies
- `.gitignore` - Files to exclude from git
- `.streamlit/secrets.toml` - Local secrets (DO NOT commit to GitHub)

## 🔐 Security Notes

- Never commit `.streamlit/secrets.toml` to GitHub
- Always use Streamlit Cloud's secrets management for credentials
- The `.gitignore` file ensures sensitive files are not uploaded

## 🔧 Local Development

To run locally:

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set up Google Cloud credentials
export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"

# Run the dashboard
streamlit run recovery_dashboard.py
```

## 📊 Dashboard Features

- **Executive Summary**: Key performance indicators and profitability metrics
- **Operations**: Platform performance and issue type analysis
- **Financial Impact**: Recovery trends and financial breakdown
- **Trends & Analytics**: Historical trends and cohort analysis

## 🆓 Free Hosting Benefits

Streamlit Cloud offers:
- Free hosting for public repositories
- Automatic HTTPS/SSL
- Easy secrets management
- Automatic redeployment on git push
- Custom subdomain (yourapp.streamlit.app)