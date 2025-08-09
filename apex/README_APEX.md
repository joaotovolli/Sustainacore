# APEX Wiring (minimal)
1) Add a Textarea (PXX_QUESTION) + Button
2) Page → Execute when Page Loads (Function and Global Variable Declaration):
```js
window.askAPI = async function(question){
  const res = await fetch('http://<VM_PUBLIC_IP>:8080/ask', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({question, top_k:3})
  });
  return res.json();
}
```
3) Button → Dynamic Action → JavaScript Code:
```js
askAPI($v('PXX_QUESTION')).then(({answer})=>{
  apex.message.showPageSuccess(answer);
}).catch(e=>apex.message.alert('Error: '+e));
```
