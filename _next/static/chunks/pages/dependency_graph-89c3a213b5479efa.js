(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[755],{56029:function(e,t,n){var r=n(33448);e.exports=function(e,t,n){for(var l=-1,o=e.length;++l<o;){var i=e[l],a=t(i);if(null!=a&&(void 0===s?a==a&&!r(a):n(a,s)))var s=a,c=i}return c}},53325:function(e){e.exports=function(e,t){return e>t}},6162:function(e,t,n){var r=n(56029),l=n(53325),o=n(6557);e.exports=function(e){return e&&e.length?r(e,o,l):void 0}},31351:function(e){var t=Array.prototype.reverse;e.exports=function(e){return null==e?e:t.call(e)}},61603:function(e,t,n){(window.__NEXT_P=window.__NEXT_P||[]).push(["/dependency_graph",function(){return n(55757)}])},62568:function(e,t,n){"use strict";n.d(t,{D:function(){return f},r:function(){return u}});var r=n(85893),l=n(79351),o=n(47741),i=n(41664),a=n.n(i),s=n(95100),c=n(664),d=n(7963);function u(e){let[t,n]=(0,s.v1)("modalId",s.U);return[null==e?void 0:e.find(e=>e.id===t),n]}function f(e){let{modalData:t,onClose:n}=e;return(0,r.jsxs)(l.u_,{isOpen:void 0!==t,onClose:n,size:"3xl",children:[(0,r.jsx)(l.ZA,{}),(0,r.jsxs)(l.hz,{children:[(0,r.jsxs)(l.xB,{children:["Catalog of ",t&&(0,c.ks)(t)," ",null==t?void 0:t.id," - ",null==t?void 0:t.name]}),(0,r.jsx)(l.ol,{}),(0,r.jsx)(l.fe,{children:t&&(0,r.jsx)(d.Rm,{src:t,collapsed:1,name:null,displayDataTypes:!1})}),(0,r.jsxs)(l.mz,{children:[t&&(0,c.vx)(t)&&(0,r.jsx)(o.zx,{colorScheme:"blue",mr:3,children:(0,r.jsx)(a(),{href:"/fragment_graph/?id=".concat(t.id),children:"View Fragments"})}),(0,r.jsx)(o.zx,{mr:3,onClick:n,children:"Close"})]})]})]})}},7963:function(e,t,n){"use strict";n.d(t,{Rm:function(){return M},KB:function(){return A},Kf:function(){return G},gU:function(){return z},vk:function(){return D},vP:function(){return I},sW:function(){return T}});var r=n(85893),l=n(47741),o=n(40639),i=n(67294),a=n(32067),s=n(54520),c=n(28387),d=(...e)=>e.filter(Boolean).join(" "),[u,f]=(0,c.k)({name:"TableStylesContext",errorMessage:"useTableStyles returned is 'undefined'. Seems you forgot to wrap the components in \"<Table />\" "}),h=(0,a.Gp)((e,t)=>{let n=(0,a.jC)("Table",e),{className:r,...l}=(0,s.Lr)(e);return i.createElement(u,{value:n},i.createElement(a.m$.table,{role:"table",ref:t,__css:n.table,className:d("chakra-table",r),...l}))});h.displayName="Table";var p=(0,a.Gp)((e,t)=>{let{overflow:n,overflowX:r,className:l,...o}=e;return i.createElement(a.m$.div,{ref:t,className:d("chakra-table__container",l),...o,__css:{display:"block",whiteSpace:"nowrap",WebkitOverflowScrolling:"touch",overflowX:n??r??"auto",overflowY:"hidden",maxWidth:"100%"}})});(0,a.Gp)((e,t)=>{let{placement:n="bottom",...r}=e,l=f();return i.createElement(a.m$.caption,{...r,ref:t,__css:{...l.caption,captionSide:n}})}).displayName="TableCaption";var m=(0,a.Gp)((e,t)=>{let n=f();return i.createElement(a.m$.thead,{...e,ref:t,__css:n.thead})}),x=(0,a.Gp)((e,t)=>{let n=f();return i.createElement(a.m$.tbody,{...e,ref:t,__css:n.tbody})});(0,a.Gp)((e,t)=>{let n=f();return i.createElement(a.m$.tfoot,{...e,ref:t,__css:n.tfoot})});var g=(0,a.Gp)(({isNumeric:e,...t},n)=>{let r=f();return i.createElement(a.m$.th,{...t,ref:n,__css:r.th,"data-is-numeric":e})}),w=(0,a.Gp)((e,t)=>{let n=f();return i.createElement(a.m$.tr,{role:"row",...e,ref:t,__css:n.tr})}),v=(0,a.Gp)(({isNumeric:e,...t},n)=>{let r=f();return i.createElement(a.m$.td,{role:"gridcell",...t,ref:n,__css:r.td,"data-is-numeric":e})}),j=n(63679),y=n(9008),b=n.n(y),_=n(41664),k=n.n(_),S=n(6448);function C(e){var t,n,r,l;return"columnDesc"in e?"".concat(null===(t=e.columnDesc)||void 0===t?void 0:t.name," (").concat(null===(r=e.columnDesc)||void 0===r?void 0:null===(n=r.columnType)||void 0===n?void 0:n.typeName,")"):"".concat(e.name," (").concat(null===(l=e.dataType)||void 0===l?void 0:l.typeName,")")}var E=n(49023),N=n(62568);let M=(0,j.ZP)(()=>n.e(171).then(n.t.bind(n,55171,23))),D={name:"Depends",width:1,content:e=>(0,r.jsx)(k(),{href:"/dependency_graph/?id=".concat(e.id),children:(0,r.jsx)(l.zx,{size:"sm","aria-label":"view dependents",colorScheme:"blue",variant:"link",children:"D"})})},I={name:"Primary Key",width:1,content:e=>e.pk.map(e=>e.columnIndex).map(t=>e.columns[t]).map(e=>C(e)).join(", ")},z={name:"Connector",width:3,content:e=>{var t;return null!==(t=e.withProperties.connector)&&void 0!==t?t:"unknown"}},G={name:"Connector",width:3,content:e=>{var t;return null!==(t=e.properties.connector)&&void 0!==t?t:"unknown"}},T=[D,{name:"Fragments",width:1,content:e=>(0,r.jsx)(k(),{href:"/fragment_graph/?id=".concat(e.id),children:(0,r.jsx)(l.zx,{size:"sm","aria-label":"view fragments",colorScheme:"blue",variant:"link",children:"F"})})}];function A(e,t,n){let{response:a}=(0,E.Z)(t),[s,c]=(0,N.r)(a),d=(0,r.jsx)(N.D,{modalData:s,onClose:()=>c(null)}),u=(0,r.jsxs)(o.xu,{p:3,children:[(0,r.jsx)(S.Z,{children:e}),(0,r.jsx)(p,{children:(0,r.jsxs)(h,{variant:"simple",size:"sm",maxWidth:"full",children:[(0,r.jsx)(m,{children:(0,r.jsxs)(w,{children:[(0,r.jsx)(g,{width:3,children:"Id"}),(0,r.jsx)(g,{width:5,children:"Name"}),(0,r.jsx)(g,{width:3,children:"Owner"}),n.map(e=>(0,r.jsx)(g,{width:e.width,children:e.name},e.name)),(0,r.jsx)(g,{children:"Visible Columns"})]})}),(0,r.jsx)(x,{children:null==a?void 0:a.map(e=>(0,r.jsxs)(w,{children:[(0,r.jsx)(v,{children:(0,r.jsx)(l.zx,{size:"sm","aria-label":"view catalog",colorScheme:"blue",variant:"link",onClick:()=>c(e.id),children:e.id})}),(0,r.jsx)(v,{children:e.name}),(0,r.jsx)(v,{children:e.owner}),n.map(t=>(0,r.jsx)(v,{children:t.content(e)},t.name)),(0,r.jsx)(v,{overflowWrap:"normal",children:e.columns.filter(e=>!("isHidden"in e)||!e.isHidden).map(e=>C(e)).join(", ")})]},e.id))})]})})]});return(0,r.jsxs)(i.Fragment,{children:[(0,r.jsx)(b(),{children:(0,r.jsx)("title",{children:e})}),d,u]})}},93451:function(e,t,n){"use strict";n.d(t,{A8:function(){return o},AJ:function(){return a},Sx:function(){return i},jI:function(){return s}});var r=n(6162),l=n.n(r);function o(e,t,n){let r=function(e){let t=new Map;for(let n of e)t.set(n.id,n);let n=new Map,r=new Map,l=e=>{let o=r.get(e);if(void 0!==o)return o;let i={nextNodes:[]},a=t.get(e);if(void 0===a)throw Error("no such id ".concat(e));for(let e of a.parentIds)l(e).nextNodes.push(i);return r.set(e,i),n.set(i,e),i};for(let t of e)l(t.id);let o=new Map,i=[];for(let e of n.keys())i.push(e);for(let e of function(e){let t=[],n=[],r=new Map,l=e=>{if(e.temp)throw Error("This is not a DAG");if(!e.perm){e.temp=!0;let n=-1;for(let t of e.node.nextNodes){r.get(t).isInput=!1,e.isOutput=!1;let o=l(r.get(t));o>n&&(n=o)}e.temp=!1,e.perm=!0,e.g=n+1,t.unshift(e.node)}return e.g};for(let t of e){let e={node:t,temp:!1,perm:!1,isInput:!0,isOutput:!0,g:0};r.set(t,e),n.push(e)}let o=0;for(let e of n){let t=l(e);t>o&&(o=t)}for(let e of n)e.g=o-e.g;let i=[];for(let e=0;e<o+1;++e)i.push({nodes:[],occupyRow:new Set});let a=new Map,s=new Map;for(let e of n)i[e.g].nodes.push(e.node),a.set(e.node,e.g);let c=(e,t)=>{s.set(e,t),i[a.get(e)].occupyRow.add(t)},d=(e,t,n)=>{for(let r=e;r<=t;++r)i[r].occupyRow.add(n)},u=(e,t)=>i[e].occupyRow.has(t),f=(e,t,n)=>{if(n<0)return!1;for(let r=e;r<=t;++r)if(u(r,n))return!0;return!1};for(let t of e)t.nextNodes.sort((e,t)=>a.get(t)-a.get(e));for(let e of i)for(let t of e.nodes){if(!s.has(t)){for(let e of t.nextNodes){if(s.has(e))continue;let n=-1;for(;f(a.get(t),a.get(e),++n););c(t,n),c(e,n),d(a.get(t)+1,a.get(e)-1,n);break}if(!s.has(t)){let e=-1;for(;u(a.get(t),++e););c(t,e)}}for(let e of t.nextNodes){if(s.has(e))continue;let n=s.get(t);if(!f(a.get(t)+1,a.get(e),n)){c(e,n),d(a.get(t)+1,a.get(e)-1,n);continue}for(n=-1;f(a.get(t)+1,a.get(e),++n););c(e,n),d(a.get(t)+1,a.get(e)-1,n)}}let h=new Map;for(let t of e)h.set(t,[a.get(t),s.get(t)]);return h}(i)){let r=n.get(e[0]);if(!r)throw Error("no corresponding item of node ".concat(e[0]));let l=t.get(r);if(!l)throw Error("item id ".concat(r," is not present in idToBox"));o.set(l,e[1])}return o}(e),o=new Map,i=new Map,a=0,s=0;for(let e of r){let t=e[0],n=e[1][0],r=e[1][1],c=o.get(n)||0;t.width>c&&o.set(n,t.width);let d=i.get(r)||0;t.height>d&&i.set(r,t.height),a=l()([n,a])||0,s=l()([r,s])||0}let c=new Map,d=new Map,u=(e,t,n,r)=>{let l=n.get(e);if(l)return l;if(0===e)l=0;else{let o=r.get(e-1);if(!o)throw Error("".concat(e-1," has no result"));l=u(e-1,t,n,r)+o+t}return n.set(e,l),l};for(let e=0;e<=a;++e)u(e,t,c,o);for(let e=0;e<=s;++e)u(e,n,d,i);let f=[];for(let[e,[t,n]]of r){let r=c.get(t),l=d.get(n);if(void 0!==r&&void 0!==l)f.push({x:r,y:l,...e});else throw Error("x of layer ".concat(t,": ").concat(r,", y of row ").concat(n,": ").concat(l," "))}return f}function i(e,t,n,r){return o(e,n,t).map(e=>{let{x:t,y:n,...l}=e;return{x:t+r,y:n+r,...l}}).map(e=>{let{x:t,y:n,...r}=e;return{x:n,y:t,...r}})}function a(e){let t=[],n=new Map;for(let t of e)n.set(t.id,t);for(let r of e)for(let e of r.parentIds){let l=n.get(e);t.push({points:[{x:r.x,y:r.y},{x:l.x,y:l.y}],source:r.id,target:e})}return t}function s(e){let t=[],n=new Map;for(let t of e)n.set(t.id,t);for(let r of e){for(let e of r.parentIds){let l=n.get(e);t.push({points:[{x:r.x+r.width/2,y:r.y+r.height/2},{x:l.x+l.width/2,y:l.y+l.height/2}],source:r.id,target:e})}for(let e of r.externalParentIds)t.push({points:[{x:r.x,y:r.y+r.height/2},{x:r.x+100,y:r.y+r.height/2}],source:r.id,target:e})}return t}},55757:function(e,t,n){"use strict";n.r(t),n.d(t,{default:function(){return _}});var r=n(85893),l=n(40639),o=n(47741),i=n(31351),a=n.n(i),s=n(89734),c=n.n(s),d=n(9008),u=n.n(d),f=n(95100),h=n(67294),p=n(52189),m=n(79855),x=n(93451),g=n(664),w=n(62568);function v(e){let{nodes:t,selectedId:n,setSelectedId:l}=e,[o,i]=(0,w.r)(t.map(e=>e.relation)),a=(0,h.useRef)(null),{layoutMap:s,width:c,height:d,links:u}=(0,h.useCallback)(()=>{let e=(0,x.Sx)(t,50,50,12).map(e=>{let{x:t,y:n,...r}=e;return{x:t+50,y:n+50,...r}}),n=(0,x.AJ)(e),{width:r,height:l}=function(e,t){let n=0,r=0;for(let{x:l,y:o}of e)n=Math.max(n,l+t),r=Math.max(r,o+t);return{width:n,height:r}}(e,12);return{layoutMap:e,links:n,width:r+50+100,height:l+50+100}},[t])();return(0,h.useEffect)(()=>{let e=a.current,t=m.Ys(e),r=m.ak_,o=m.jvg().curve(r).x(e=>{let{x:t}=e;return t}).y(e=>{let{y:t}=e;return t}),c=t.select(".edges").selectAll(".edge").data(u),d=e=>e===n,f=e=>e.attr("d",e=>{let{points:t}=e;return o(t)}).attr("fill","none").attr("stroke-width",1).attr("stroke-width",e=>d(e.source)||d(e.target)?4:2).attr("opacity",e=>d(e.source)||d(e.target)?1:.5).attr("stroke",e=>d(e.source)||d(e.target)?p.rS.colors.blue["500"]:p.rS.colors.gray["300"]);c.exit().remove(),c.enter().call(e=>e.append("path").attr("class","edge").call(f)),c.call(f);let h=e=>{e.attr("transform",e=>{let{x:t,y:n}=e;return"translate(".concat(t,",").concat(n,")")});let t=e.select("circle");t.empty()&&(t=e.append("circle")),t.attr("r",12).attr("fill",e=>{let{id:t,relation:n}=e,r=(0,g.vx)(n)?"500":"400";return d(t)?p.rS.colors.blue[r]:p.rS.colors.gray[r]});let n=e.select(".text");n.empty()&&(n=e.append("text").attr("class","text")),n.attr("fill","black").text(e=>{let{name:t}=e;return t}).attr("font-family","inherit").attr("text-anchor","middle").attr("dy",24).attr("font-size",12).attr("transform","rotate(-8)");let r=e.select(".type");r.empty()&&(r=e.append("text").attr("class","type"));let o=e=>{let t=(0,g.Mk)(e);return"SINK"===t?"K":t.charAt(0)};r.attr("fill","white").text(e=>{let{relation:t}=e;return"".concat(o(t))}).attr("font-family","inherit").attr("text-anchor","middle").attr("dy",6).attr("font-size",16).attr("font-weight","bold");let a=e.select("title");return a.empty()&&(a=e.append("title")),a.text(e=>{let{relation:t}=e;return"".concat(t.name," (").concat((0,g.ks)(t),")")}),e.style("cursor","pointer").on("click",(e,t)=>{let{relation:n,id:r}=t;l(r),i(n.id)}),e},x=t.select(".boxes").selectAll(".node").data(s);x.enter().call(e=>e.append("g").attr("class","node").call(h)),x.call(h),x.exit().remove()},[s,u,n,i,l]),(0,r.jsxs)(r.Fragment,{children:[(0,r.jsxs)("svg",{ref:a,width:"".concat(c,"px"),height:"".concat(d,"px"),children:[(0,r.jsx)("g",{className:"edges"}),(0,r.jsx)("g",{className:"boxes"})]}),(0,r.jsx)(w.D,{modalData:o,onClose:()=>i(null)})]})}var j=n(6448),y=n(49023);let b="200px";function _(){let{response:e}=(0,y.Z)(g.H4),[t,n]=(0,f.v1)("id",f.U),i=(0,h.useCallback)(()=>e?function(e){let t=[],n=new Set(e.map(e=>e.id));for(let r of a()(c()(e,"id")))t.push({id:r.id.toString(),name:r.name,parentIds:(0,g.vx)(r)?r.dependentRelations.filter(e=>n.has(e)).map(e=>e.toString()):[],order:r.id,width:24,height:24,relation:r});return t}(e):void 0,[e])(),s=(0,r.jsxs)(l.kC,{p:3,height:"calc(100vh - 20px)",flexDirection:"column",children:[(0,r.jsx)(j.Z,{children:"Dependency Graph"}),(0,r.jsxs)(l.kC,{flexDirection:"row",height:"full",children:[(0,r.jsxs)(l.kC,{width:b,height:"full",maxHeight:"full",mr:3,alignItems:"flex-start",flexDirection:"column",children:[(0,r.jsx)(l.xv,{fontWeight:"semibold",mb:3,children:"Relations"}),(0,r.jsx)(l.xu,{flex:1,overflowY:"scroll",children:(0,r.jsx)(l.gC,{width:b,align:"start",spacing:1,children:null==e?void 0:e.map(e=>{let l=t===e.id;return(0,r.jsx)(o.zx,{colorScheme:l?"blue":"gray",color:l?"blue.600":"gray.500",variant:l?"outline":"ghost",py:0,height:8,justifyContent:"flex-start",onClick:()=>n(e.id),children:e.name},e.id)})})})]}),(0,r.jsxs)(l.xu,{flex:1,height:"full",ml:3,overflowX:"scroll",overflowY:"scroll",children:[(0,r.jsx)(l.xv,{fontWeight:"semibold",children:"Dependency Graph"}),i&&(0,r.jsx)(v,{nodes:i,selectedId:null==t?void 0:t.toString(),setSelectedId:e=>n(parseInt(e))})]})]})]});return(0,r.jsxs)(h.Fragment,{children:[(0,r.jsx)(u(),{children:(0,r.jsx)("title",{children:"Streaming Graph"})}),s]})}}},function(e){e.O(0,[662,679,184,667,591,855,970,661,888,774,179],function(){return e(e.s=61603)}),_N_E=e.O()}]);