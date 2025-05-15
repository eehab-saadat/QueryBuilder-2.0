import React, { useState } from 'react';

const mockTableColumns = {
  ProblemDim: ['ProblemID', 'ProblemName', 'Severity'],
  ProceduresDim: ['ProcedureID', 'ProcedureName', 'Cost'],
};

function TableColumnSelector() {
  const dummyColumnData = {
  ProblemDim: {
    ProblemID: ['P001', 'P002', 'P003'],
    ProblemName: ['Headache', 'Cough', 'Fever', 'Migraine','melon'],
    Severity: ['Mild', 'Moderate', 'Severe'],
  },
  ProceduresDim: {
    ProcedureID: ['PR001', 'PR002', 'PR003'],
    ProcedureName: ['CT Scan', 'X-Ray', 'MRI'],
    Cost: ['100', '250', '500'],
  }
};

  const [selectedColumns, setSelectedColumns] = useState({});
  const [suggestions, setSuggestions] = useState({});
const [tableOperators, setTableOperators] = useState({});
const tableNames = Object.keys(mockTableColumns);
const handleOperatorChange = (index, newOperator) => {
  setTableOperators(prev => ({ ...prev, [index]: newOperator }));
};

  const [dropdownOpen, setDropdownOpen] = useState({});
  const [columnValues, setColumnValues] = useState({});
  const handleSuggestionSelect = (table, column, value) => {
  const key = `${table}_${column}`;
  setColumnValues(prev => ({ ...prev, [key]: value }));
  setSuggestions(prev => ({ ...prev, [key]: [] }));
};

  const handleSelect = (table, column) => {
  if (!column) return;
  setSelectedColumns(prev => {
    const current = prev[table] || [];
    if (!current.includes(column)) {
      const updated = { ...prev, [table]: [...current, column] };

      // Pre-fill suggestions for the newly added column
      const key = `${table}_${column}`;
      const allValues = dummyColumnData?.[table]?.[column] || [];
      setSuggestions(prev => ({ ...prev, [key]: allValues }));

      return updated;
    }
    return prev;
  });
  setDropdownOpen(prev => ({ ...prev, [table]: false }));
};

  const handleRemove = (table, column) => {
    setSelectedColumns(prev => {
      return { ...prev, [table]: prev[table].filter(col => col !== column) };
    });
  };

const handleInputChange = (table, column, value) => {
  const key = `${table}_${column}`;
  setColumnValues(prev => ({ ...prev, [key]: value }));

  const allValues = dummyColumnData?.[table]?.[column] || [];
  const filtered = allValues.filter(item =>
    item.toLowerCase().startsWith(value.toLowerCase())
  );
  setSuggestions(prev => ({ ...prev, [key]: filtered }));
};



  const toggleDropdown = (table) => {
    setDropdownOpen(prev => ({ ...prev, [table]: !prev[table] }));
  };

  return (
    <div style={{ padding: '24px', backgroundColor: 'transparent'}}>
      <h1 style={{ fontSize: '24px', fontWeight: 'bold', marginBottom: '24px', color: '#1f2937', textAlign: 'center' }}>
        Filters
      </h1>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
       {Object.keys(mockTableColumns).map((table,index) => (
  <div style={{display:'flex',flexDirection:'column'}}>

  <div
    key={table}
    style={{
      backgroundColor: 'transparent',
      padding: '16px',
      borderRadius: '8px',
                    boxShadow: '0 2px 6px rgba(0,0,0,0.3)',
      display: 'flex',
      alignItems: 'center',
      gap: '12px',
      overflowX: 'auto',
    }}
  >
    {/* Table Name */}
    <h2 style={{ fontSize: '16px', fontWeight: '600', color: '#4b5563', whiteSpace: 'nowrap', margin: 0 }}>
      {table}
    </h2>

    {/* + Button */}
    <button
      onClick={() => toggleDropdown(table)}
      style={{
        width: '28px',
height: '28px',

        backgroundColor: 'rgba(255, 255, 255, 0.1)', // semi-transparent white
            color: 'black',
                    boxShadow: '0 2px 6px rgba(0,0,0,0.5)',
        fontSize: '20px',
        cursor: 'pointer',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        flexShrink: 0,
      }}
      onMouseEnter={(e) => e.target.style.backgroundColor = '#059669'}
      onMouseLeave={(e) => e.target.style.backgroundColor = 'rgba(255, 255, 255, 0.1)'}
    >
      +
    </button>

    {/* Dropdown */}
    {dropdownOpen[table] && (
      <div
        style={{
          position: 'absolute',
          backgroundColor: 'white',
          border: '1px solid #ddd',
          borderRadius: '8px',
          boxShadow: '0 2px 6px rgba(0,0,0,0.1)',
          zIndex: 10,
          marginTop: '160px',
        }}
      >
        {mockTableColumns[table].map(column => (
          <div
            key={column}
            style={{
              padding: '10px 16px',
              cursor: 'pointer',
              fontSize: '14px',
              transition: 'background-color 0.3s ease',
            }}
            onClick={() => handleSelect(table, column)}
            onMouseEnter={(e) => e.target.style.backgroundColor = '#f3f4f6'}
            onMouseLeave={(e) => e.target.style.backgroundColor = '#fff'}
          >
            {column}
          </div>
        ))}
      </div>
    )}

    {/* Column Inputs in the Same Row */}
    {(selectedColumns[table] || []).map(col => (
      <div
        key={col}
        style={{
  display: 'flex',
  alignItems: 'center',
  backgroundColor: 'transparent',
  padding: '4px 8px',
  borderRadius: '16px',
  minWidth: '220px',
  flexShrink: 0,
  border: '1px solid #d1d5db', // light gray border
  boxShadow: '0 2px 6px rgba(0, 0, 0, 0.1)', // subtle shadow
}}
      >
        <span style={{ fontSize: '14px', color: '#333', whiteSpace: 'nowrap' }}>{col} =</span>

        <div style={{ display: 'flex', flexDirection: 'column', flex: 1 }}>
  <input
    type="text"
    placeholder={`Enter value for ${col}`}
    value={columnValues[`${table}_${col}`] || ''}
    onChange={(e) => handleInputChange(table, col, e.target.value)}
   style={{
  padding: '8px 12px',
  border: '1px solid #ddd',
  borderRadius: '8px',
  fontSize: '14px',
  margin: '8px',
  width: '100%',
  backgroundColor: 'rgba(255, 255, 255, 0.5)', // semi-transparent white
}}
  />

  {/* Suggestions inside natural flow, with scroll */}
  {suggestions[`${table}_${col}`]?.length > 0 && (
    <div
      style={{
        marginTop: '4px',
        backgroundColor: 'rgba(255, 255, 255, 0.5)',
        border: '1px solid #ccc',
        borderRadius: '8px',
        boxShadow: '0 2px 6px rgba(0,0,0,0.1)',
        maxHeight: '150px',
        overflowY: 'auto',
        zIndex: 1,
      }}
    >
      {suggestions[`${table}_${col}`].map((val, idx) => (
        <div
          key={idx}
          style={{
            padding: '8px 12px',
            cursor: 'pointer',
            fontSize: '14px',
            whiteSpace: 'nowrap',
          }}
          onClick={() => handleSuggestionSelect(table, col, val)}
          onMouseEnter={(e) => e.target.style.backgroundColor = '#f3f4f6'}
          onMouseLeave={(e) => e.target.style.backgroundColor = '#fff'}
        >
          {val}
        </div>
      ))}
    </div>
  )}
</div>




        <button
          onClick={() => handleRemove(table, col)}
          style={{
            marginLeft: '15px',
            width: '22px',
            height: '22px',
            backgroundColor: 'rgba(255, 255, 255, 0.1)', // semi-transparent white
            color: 'black',
                    boxShadow: '0 2px 6px rgba(0,0,0,0.5)',

            border: 'none',
            fontSize: '14px',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            
            justifyContent: 'center',
            flexShrink: 0,
          }}
          onMouseEnter={(e) => e.target.style.backgroundColor = '#dc2626'}
          onMouseLeave={(e) => e.target.style.backgroundColor = ' rgba(255, 255, 255, 0.1)'}
        >
          x
        </button>
      </div>
    ))}

  </div>

  
    {index < tableNames.length - 1 && (
  <div
    style={{
      width: '100%',
      margin: '16px 0',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      gap: '12px',
    }}
  >
    <div
      style={{
        height: '1px',
        backgroundColor: '#e5e7eb',
        flexGrow: 1,
      }}
    />

    <select
      value={tableOperators[index] || 'AND'}
      onChange={(e) => handleOperatorChange(index, e.target.value)}
      style={{
        padding: '6px 12px',
        borderRadius: '8px',
        border: '1px solid #ccc',
        fontSize: '14px',
        backgroundColor: '#fff',
        color: '#111827',
        cursor: 'pointer',
        boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
      }}
    >
      <option value="AND">AND</option>
      <option value="OR">OR</option>
      <option value="NOT">NOT</option>
    </select>

    <div
      style={{
        height: '1px',
        backgroundColor: '#e5e7eb',
        flexGrow: 1,
      }}
    />
    
  </div>
  
)
}


</div>
))
}


      </div>
    </div>
  );
}

export default TableColumnSelector;
