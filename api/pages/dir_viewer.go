package pages

var DirViewerPage = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Directory Listing: %s</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        :root {
            --primary-gradient: linear-gradient(135deg, #667eea 0%%, #764ba2 100%%);
            --secondary-gradient: linear-gradient(135deg, #f093fb 0%%, #f5576c 100%%);
            --success-gradient: linear-gradient(135deg, #4facfe 0%%, #00f2fe 100%%);
            --glass-bg: rgba(255, 255, 255, 0.08);
            --glass-border: rgba(255, 255, 255, 0.15);
            --card-bg: rgba(255, 255, 255, 0.05);
            --text-primary: #ffffff;
            --text-secondary: rgba(255, 255, 255, 0.8);
            --text-muted: rgba(255, 255, 255, 0.6);
            --shadow-soft: 0 20px 40px rgba(0, 0, 0, 0.1);
            --shadow-elevated: 0 30px 60px rgba(0, 0, 0, 0.15);
            --border-radius: 16px;
            --border-radius-small: 12px;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            background: var(--primary-gradient);
            min-height: 100vh;
            position: relative;
            overflow-x: hidden;
        }

        body::before {
            content: '';
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: 
                radial-gradient(circle at 20%% 80%%, rgba(120, 119, 198, 0.4) 0%%, transparent 50%%),
                radial-gradient(circle at 80%% 20%%, rgba(255, 119, 198, 0.4) 0%%, transparent 50%%),
                radial-gradient(circle at 40%% 40%%, rgba(120, 219, 255, 0.3) 0%%, transparent 50%%);
            animation: gradientShift 25s ease-in-out infinite;
            pointer-events: none;
        }

        @keyframes gradientShift {
            0%%, 100%% { opacity: 1; transform: scale(1) rotate(0deg); }
            50%% { opacity: 0.8; transform: scale(1.1) rotate(5deg); }
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 40px 24px;
            position: relative;
            z-index: 10;
        }

        .header {
            background: var(--glass-bg);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            border: 1px solid var(--glass-border);
            border-radius: var(--border-radius);
            padding: 32px;
            margin-bottom: 32px;
            box-shadow: var(--shadow-elevated);
            animation: slideDown 0.8s cubic-bezier(0.16, 1, 0.3, 1);
            position: relative;
            overflow: hidden;
        }

        .header::before {
            content: '';
            position: absolute;
            top: 0;
            left: -100%%;
            width: 100%%;
            height: 100%%;
            background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.1), transparent);
            animation: headerShine 4s ease-in-out infinite;
        }

        @keyframes headerShine {
            0%% { left: -100%%; }
            100%% { left: 100%%; }
        }

        @keyframes slideDown {
            from {
                opacity: 0;
                transform: translateY(-20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        .header-content {
            display: flex;
            align-items: center;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 24px;
            position: relative;
            z-index: 2;
        }

        .directory-info {
            display: flex;
            align-items: center;
            gap: 16px;
        }

        .folder-icon {
            width: 48px;
            height: 48px;
            background: linear-gradient(135deg, rgba(255, 255, 255, 0.2), rgba(255, 255, 255, 0.1));
            border-radius: var(--border-radius-small);
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: var(--shadow-soft);
            animation: float 3s ease-in-out infinite;
        }

        @keyframes float {
            0%%, 100%% { transform: translateY(0); }
            50%% { transform: translateY(-3px); }
        }

        .folder-icon svg {
            width: 24px;
            height: 24px;
            fill: var(--text-primary);
        }

        .directory-title {
            font-size: 28px;
            font-weight: 700;
            color: var(--text-primary);
            letter-spacing: -0.5px;
            margin-bottom: 4px;
        }

        .directory-path {
            font-size: 14px;
            color: var(--text-muted);
            font-family: 'SF Mono', Monaco, Consolas, monospace;
            background: rgba(255, 255, 255, 0.1);
            padding: 6px 12px;
            border-radius: 8px;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }

        .download-all {
            display: inline-flex;
            align-items: center;
            gap: 10px;
            padding: 16px 24px;
            background: var(--success-gradient);
            color: var(--text-primary);
            text-decoration: none;
            border-radius: var(--border-radius-small);
            font-weight: 600;
            font-size: 15px;
            box-shadow: var(--shadow-soft);
            transition: all 0.3s cubic-bezier(0.16, 1, 0.3, 1);
            position: relative;
            overflow: hidden;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }

        .download-all::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(45deg, rgba(255, 255, 255, 0.1), transparent);
            opacity: 0;
            transition: opacity 0.3s ease;
        }

        .download-all:hover {
            transform: translateY(-2px);
            box-shadow: 0 25px 50px rgba(0, 0, 0, 0.2);
        }

        .download-all:hover::before {
            opacity: 1;
        }

        .download-all svg {
            width: 18px;
            height: 18px;
            fill: currentColor;
        }

        .table-container {
            background: var(--glass-bg);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            border: 1px solid var(--glass-border);
            border-radius: var(--border-radius);
            overflow: hidden;
            box-shadow: var(--shadow-elevated);
            animation: slideUp 0.8s cubic-bezier(0.16, 1, 0.3, 1) 0.2s both;
        }

        @keyframes slideUp {
            from {
                opacity: 0;
                transform: translateY(20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        table {
            width: 100%%;
            border-collapse: collapse;
        }

        th {
            background: linear-gradient(135deg, rgba(255, 255, 255, 0.1), rgba(255, 255, 255, 0.05));
            color: var(--text-primary);
            font-weight: 600;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            padding: 20px 24px;
            text-align: left;
            position: sticky;
            top: 0;
            z-index: 5;
        }

        th:first-child { border-radius: 0; }
        th:last-child { border-radius: 0; }

        td {
            padding: 18px 24px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.08);
            color: var(--text-secondary);
            font-size: 15px;
            transition: all 0.2s ease;
        }

        tr:hover td {
            background: rgba(255, 255, 255, 0.05);
            color: var(--text-primary);
        }

        tr:last-child td {
            border-bottom: none;
        }

        .file-type {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            font-weight: 500;
        }

        .type-icon {
            width: 20px;
            height: 20px;
            border-radius: 6px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 10px;
            font-weight: 700;
            color: white;
            text-transform: uppercase;
        }

        .type-folder { background: linear-gradient(135deg, #ffd89b 0%%, #19547b 100%%); }
        .type-file { background: linear-gradient(135deg, #a8edea 0%%, #fed6e3 100%%); }

        .file-name {
            font-weight: 500;
        }

        .file-name a {
            color: var(--text-primary);
            text-decoration: none;
            transition: all 0.2s ease;
            position: relative;
        }

        .file-name a::after {
            content: '';
            position: absolute;
            bottom: -2px;
            left: 0;
            width: 0;
            height: 2px;
            background: var(--secondary-gradient);
            transition: width 0.3s ease;
        }

        .file-name a:hover {
            color: var(--text-primary);
            transform: translateX(2px);
        }

        .file-name a:hover::after {
            width: 100%%;
        }

        .file-size {
            font-family: 'SF Mono', Monaco, Consolas, monospace;
            font-size: 14px;
            color: var(--text-muted);
            font-weight: 500;
        }

        .file-hash {
            font-family: 'SF Mono', Monaco, Consolas, monospace;
            font-size: 12px;
            color: var(--text-muted);
            background: rgba(255, 255, 255, 0.05);
            padding: 6px 10px;
            border-radius: 6px;
            border: 1px solid rgba(255, 255, 255, 0.08);
            max-width: 200px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            cursor: pointer;
            transition: all 0.2s ease;
        }

        .file-hash:hover {
            background: rgba(255, 255, 255, 0.1);
            color: var(--text-secondary);
            max-width: none;
            white-space: normal;
            word-break: break-all;
        }

        @media (max-width: 768px) {
            .container {
                padding: 20px 16px;
            }

            .header {
                padding: 24px;
            }

            .header-content {
                flex-direction: column;
                align-items: stretch;
                text-align: center;
            }

            .directory-info {
                justify-content: center;
            }

            .directory-title {
                font-size: 24px;
            }

            .download-all {
                justify-content: center;
            }

            table {
                font-size: 14px;
            }

            th, td {
                padding: 12px 16px;
            }

            .file-hash {
                max-width: 120px;
            }
        }

        @media (max-width: 480px) {
            th:nth-child(3),
            td:nth-child(3) {
                display: none;
            }

            .file-hash {
                max-width: 80px;
            }
        }

        @media (prefers-reduced-motion: reduce) {
            * {
                animation-duration: 0.01ms !important;
                animation-iteration-count: 1 !important;
                transition-duration: 0.01ms !important;
            }
        }

        .loading-shimmer {
            background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.1), transparent);
            background-size: 200px 100%%;
            animation: shimmer 1.5s ease-in-out infinite;
        }

        @keyframes shimmer {
            0%% { background-position: -200px 0; }
            100%% { background-position: calc(200px + 100%%) 0; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="header-content">
                <div class="directory-info">
                    <div class="folder-icon">
                        <svg viewBox="0 0 24 24">
                            <path d="M10 4H4c-1.11 0-2 .89-2 2v12c0 1.11.89 2 2 2h16c1.11 0 2-.89 2-2V8c0-1.11-.89-2-2-2h-8l-2-2z"/>
                        </svg>
                    </div>
                    <div>
                        <h2 class="directory-title">Directory</h2>
                        <div class="directory-path">%s</div>
                    </div>
                </div>
                <a href="/download/%s" class="download-all">
                    <svg viewBox="0 0 24 24">
                        <path d="M14,2H6A2,2 0 0,0 4,4V20A2,2 0 0,0 6,22H18A2,2 0 0,0 20,20V8L14,2M18,20H6V4H13V9H18V20Z"/>
                    </svg>
                    Download All as .zip
                </a>
            </div>
        </div>

        <div class="table-container">
            <table>
                <tr>
                    <th>Type</th>
                    <th>Name</th>
                    <th>Size</th>
                    <th>Hash</th>
                </tr>
`
