import { useEffect, useState } from 'react';
import renderWithSQLBlock from './blockRenderer';
import { FiSend } from 'react-icons/fi'; // install react-icons if you haven't
import { FiChevronLeft, FiChevronRight } from 'react-icons/fi';

function ChatPage() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [chatIds, setChatIds] = useState([]);
  const [currentChatId, setCurrentChatId] = useState(1);

  const handleSend = () => {
    if (input.trim() === "") return;

    const newMessages = [...messages, { role: "user", content: input }];
    setMessages(newMessages);
    getResponse(newMessages);
    setInput("");
  };

  const getResponse = async (currentMessages) => {
    // Add a blank assistant message as placeholder
    setMessages([...currentMessages, { role: 'assistant', content: "" }]);
  
    const response = await fetch('http://127.0.0.1:8000/get-response/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages: currentMessages })
    });
  
    if (!response.ok || !response.body) {
      console.error("Streaming response failed");
      return;
    }
  
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8");
    let result = "";
  
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
  
      const chunk = decoder.decode(value, { stream: true });
      result += chunk.replace(/^data:\s*/gm, '');
  
      // Update the latest assistant message instead of adding new ones
      setMessages(prevMessages => {
        const updated = [...prevMessages];
        const lastIndex = updated.length - 1;
        if (updated[lastIndex].role === "assistant") {
          updated[lastIndex] = {
            ...updated[lastIndex],
            content: result
          };
        }
        return updated;
      });
    }
  };
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);

  
  const saveChat = (chatId, messages) => {
    fetch("http://127.0.0.1:8000/save-chats/", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat_id: chatId, messages })
    }).catch(error => console.error("Error saving chat:", error));
  };

  const addId = (chatId) => {
    fetch(`http://127.0.0.1:8000/add-chatId/${chatId}`, { method: 'GET' });
  };

  const loadChat = (chatId) => {
    saveChat(currentChatId, messages);
    fetch(`http://127.0.0.1:8000/get-chats/${chatId}`)
      .then(response => response.json())
      .then(data => {
        setMessages(data.chat);
        setCurrentChatId(chatId);
      })
      .catch(error => console.error("Error loading chat:", error));
  };

  const handleNewChat = () => {
    saveChat(currentChatId, messages);
    const newId = chatIds.length + 1;
    setChatIds([...chatIds, newId]);
    setMessages([]);
    setCurrentChatId(newId);
    addId(newId);
  };

  useEffect(() => {
    fetch('http://127.0.0.1:8000/get-chats/')
      .then(response => response.json())
      .then(data => {
        setChatIds(data.ids);
        setMessages(data.firstchat);
      });
  }, []);

  return (
    <div style={{
      display: 'flex',
      width: '100%',
      height: '100vh',
      fontFamily: 'Arial, sans-serif',
      backgroundImage: 'url("src/assets/Group41.png")',
      backgroundSize: 'cover',
      backgroundPosition: 'center',
    }}>
    
      {/* Sidebar */}
      

<div style={{
  width: isSidebarOpen ? '260px' : '0',
  overflow: 'hidden',
  transition: 'width 0.3s ease',
  backgroundSize: 'cover',
  backgroundPosition: 'left',
  //borderRight: isSidebarOpen ? '4px solid #e2e8f0' : 'none',
  padding: isSidebarOpen ? '20px' : '0',
  boxShadow: isSidebarOpen ? '1px 0 8px rgba(0, 0, 0, 0.05)' : 'none',
  display: 'flex',
  flexDirection: 'column',
}}>
  <div style={{ display: 'flex', justifyContent: 'flex-end' }}>

<button
  onClick={() => setIsSidebarOpen(!isSidebarOpen)}
  style={{
    backgroundColor:'transparent',
    position: 'absolute',
    left: isSidebarOpen ? '260px' : '0',
    color: 'black',
    outline: 'none',         // removes default focus outline
    boxShadow: 'none',       // removes box shadow if any
    transition: 'left 0.3s ease',
  }}
>
{isSidebarOpen ? <FiChevronLeft size={18} /> : <FiChevronRight size={18} />}
</button>
</div>
        <button
          onClick={handleNewChat}
          style={{
            padding: '12px',
            backgroundColor: 'transparent',// '#f0f9fe',
            //color: 'white',
            borderRadius: '10px',
            border: 'none',
            marginBottom: '20px',
            fontWeight: 'bold',
            cursor: 'pointer',
            boxShadow: '0 2px 6px rgba(79, 70, 229, 0.3)',
          }}
        >
          + New Chat
        </button>

        <div style={{ overflowY: 'auto', flex: 1 }}>
          {chatIds.map((chat, index) => (
            <div
              key={index}
              onClick={() => loadChat(chat)}
              style={{
                border:'4px solid #fddde6',
                padding: '12px 14px',
                backgroundColor: chat === currentChatId ? '#fddde6' : 'transparent',
                borderRadius: '8px',
                cursor: 'pointer',
                fontWeight: chat === currentChatId ? '600' : 'normal',
                color: '#1e293b',
                marginBottom: '8px',
                //border: chat === currentChatId ? '1px solid #6366f1' : '1px solid #e2e8f0',
                transition: 'background 0.2s ease',
              }}
            >
              Chat {chat}
            </div>
          ))}
        </div>
      </div>

      {/* Chat Area */}
      <div style={{
       
        flex: 1,
        display: 'flex',
        flexDirection: 'column',
        padding: '24px',
        backgroundColor: 'transparent'// '#f9fafb',
      }}>
        {/* Chat Messages */}
        <div style={{
          flex: 1,
          overflowY: 'auto',
          
          marginBottom: '16px',
          paddingRight: '12px',
        }}>
          {messages.length === 0 ? (
            <div style={{ textAlign: 'center', marginTop: '100px', color: '#6b7280' }}>
              <h2>No messages yet</h2>
              <p>Start a conversation to see messages appear here.</p>
            </div>
          ) : (
            messages.filter((message) => 'role' in message).map((message, index) => {
              const isUser = message.role === 'user';
              const content = isUser
                ? message.content
                : Array.isArray(message.content)
                ? message.content[0]?.text
                : message.content;

              return (
                <div
                  key={index}
                  style={{
                    display: 'flex',
                    justifyContent: isUser ? 'flex-end' : 'flex-start',
                    marginBottom: '12px',
                  }}
                >
                  <div
                    style={{
                      backgroundColor: isUser ? 'transparent' : 'transparent',// '#fddde6',
                      //color: isUser ? 'white' : '#1e293b',
                      padding: '12px 16px',
                      borderRadius: '16px',
                      maxWidth: '70%',
                      boxShadow: isUser? '0 2px 5px rgba(0,0,0,0.1)':null,
                      fontSize: '15px',
                      lineHeight: '1.4',
                    }}
                  >
                    {renderWithSQLBlock(content)}
                  </div>
                </div>
              );
            })
          )}
        </div>

        {/* Input Box */}
        <div style={{
          display: 'flex',
          alignItems: 'center',
          backgroundColor: 'transparent',// '#fff',
          padding: '8px 12px',
          opacity:'1',
          borderRadius: '24px',
          boxShadow: '0 0 8px rgba(0, 0, 0, 0.05)',
          gap: '8px',
          width: '100%',
        }}>
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Type your message..."
            style={{
              flex: 1,
              border: 'none',
              outline: 'none',
              fontSize: '16px',
              padding: '8px',
              borderRadius: '20px',
              backgroundColor: 'transparent',
            }}
          />
          <button
            onClick={handleSend}
            style={{
              backgroundColor: '#fddde6',
              border: 'none',
              borderRadius: '50%',
              padding: '8px',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <FiSend size={18} /> 
            
          </button>
        </div>
      </div>
    </div>
  );
}

export default ChatPage;
