import React from "react";
import { Light as SyntaxHighlighter } from "react-syntax-highlighter";
import sql from "react-syntax-highlighter/dist/esm/languages/hljs/sql";
import docco from "react-syntax-highlighter/dist/esm/styles/hljs/docco";

SyntaxHighlighter.registerLanguage("sql", sql);
const renderWithSQLBlock = (text) => {
  const lines = text.split('\n');
  const elements = [];
  let inSQLBlock = false;
  let sqlContent = '';
  let key = 0;

  const handleCopy = (sql) => {
    navigator.clipboard.writeText(sql).then(() => {
      alert("Copied to clipboard!");
    });
  };

  for (let line of lines) {
    if (line.trim().startsWith("```sql")) {
      inSQLBlock = true;
      sqlContent = '';
    } else if (line.trim() === "```" && inSQLBlock) {
      // End of SQL block
      elements.push(
        <div
          key={key++}
          style={{
            position: 'relative',
            background: 'transparent', //' #f4f4f4',
            margin: '1rem',
            padding: '1rem',
            borderRadius: '5px',
            border:'4px solid #fddde6',
            overflowX: 'auto',
            textAlign: 'left',
          }}
        >
          <button
            onClick={() => handleCopy(sqlContent)}
            style={{
              position: 'absolute',
              top: '10px',
              right: '10px',
              backgroundColor: '#fddde6',
              //color: 'white',
              border: 'none',
              padding: '4px 8px',
              borderRadius: '4px',
              cursor: 'pointer',
              fontSize: '12px',
            }}
          >
            Copy
          </button>
          <pre style={{ margin: 0 }}>
            <code>{sqlContent}</code>
          </pre>
        </div>
      );
      inSQLBlock = false;
    } else if (inSQLBlock) {
      sqlContent += line + '\n';
    } else {
      // Normal text line
      elements.push(<p style={{ textAlign: 'left' }} key={key++}>{line}</p>);
    }
  }

  // If still inside an unclosed SQL block (streaming not finished yet)
  if (inSQLBlock) {
    elements.push(
      <div
        key={key++}
        style={{
          position: 'relative',
          background: '#f4f4f4',
          margin: '1rem',
          padding: '1rem',
          borderRadius: '5px',
          overflowX: 'auto',
          textAlign: 'left',
        }}
      >
        <button
          onClick={() => handleCopy(sqlContent)}
          style={{
            position: 'absolute',
            top: '10px',
            right: '10px',
            backgroundColor: '#f0f9fe',
            color: 'white',
            border: 'none',
            padding: '4px 8px',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '12px',
          }}
        >
          Copy
        </button>
        <pre style={{ margin: 0 }}>
          <code>{sqlContent}</code>
        </pre>
      </div>
    );
  }

  return elements;
};

export default renderWithSQLBlock;
