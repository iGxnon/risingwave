(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[939],{92560:function(e,n,t){(window.__NEXT_P=window.__NEXT_P||[]).push(["/materialized_views",function(){return t(48001)}])},62568:function(e,n,t){"use strict";t.d(n,{D:function(){return m},r:function(){return u}});var r=t(85893),i=t(79351),l=t(47741),c=t(41664),a=t.n(c),o=t(95100),s=t(664),d=t(7963);function u(e){let[n,t]=(0,o.v1)("modalId",o.U);return[null==e?void 0:e.find(e=>e.id===n),t]}function m(e){let{modalData:n,onClose:t}=e;return(0,r.jsxs)(i.u_,{isOpen:void 0!==n,onClose:t,size:"3xl",children:[(0,r.jsx)(i.ZA,{}),(0,r.jsxs)(i.hz,{children:[(0,r.jsxs)(i.xB,{children:["Catalog of ",n&&(0,s.ks)(n)," ",null==n?void 0:n.id," - ",null==n?void 0:n.name]}),(0,r.jsx)(i.ol,{}),(0,r.jsx)(i.fe,{children:n&&(0,r.jsx)(d.Rm,{src:n,collapsed:1,name:null,displayDataTypes:!1})}),(0,r.jsxs)(i.mz,{children:[n&&(0,s.vx)(n)&&(0,r.jsx)(l.zx,{colorScheme:"blue",mr:3,children:(0,r.jsx)(a(),{href:"/fragment_graph/?id=".concat(n.id),children:"View Fragments"})}),(0,r.jsx)(l.zx,{mr:3,onClick:t,children:"Close"})]})]})]})}},7963:function(e,n,t){"use strict";t.d(n,{Rm:function(){return T},KB:function(){return W},Kf:function(){return $},gU:function(){return G},vk:function(){return S},vP:function(){return D},sW:function(){return P}});var r=t(85893),i=t(47741),l=t(40639),c=t(67294),a=t(32067),o=t(54520),s=t(28387),d=(...e)=>e.filter(Boolean).join(" "),[u,m]=(0,s.k)({name:"TableStylesContext",errorMessage:"useTableStyles returned is 'undefined'. Seems you forgot to wrap the components in \"<Table />\" "}),h=(0,a.Gp)((e,n)=>{let t=(0,a.jC)("Table",e),{className:r,...i}=(0,o.Lr)(e);return c.createElement(u,{value:t},c.createElement(a.m$.table,{role:"table",ref:n,__css:t.table,className:d("chakra-table",r),...i}))});h.displayName="Table";var p=(0,a.Gp)((e,n)=>{let{overflow:t,overflowX:r,className:i,...l}=e;return c.createElement(a.m$.div,{ref:n,className:d("chakra-table__container",i),...l,__css:{display:"block",whiteSpace:"nowrap",WebkitOverflowScrolling:"touch",overflowX:t??r??"auto",overflowY:"hidden",maxWidth:"100%"}})});(0,a.Gp)((e,n)=>{let{placement:t="bottom",...r}=e,i=m();return c.createElement(a.m$.caption,{...r,ref:n,__css:{...i.caption,captionSide:t}})}).displayName="TableCaption";var x=(0,a.Gp)((e,n)=>{let t=m();return c.createElement(a.m$.thead,{...e,ref:n,__css:t.thead})}),f=(0,a.Gp)((e,n)=>{let t=m();return c.createElement(a.m$.tbody,{...e,ref:n,__css:t.tbody})});(0,a.Gp)((e,n)=>{let t=m();return c.createElement(a.m$.tfoot,{...e,ref:n,__css:t.tfoot})});var v=(0,a.Gp)(({isNumeric:e,...n},t)=>{let r=m();return c.createElement(a.m$.th,{...n,ref:t,__css:r.th,"data-is-numeric":e})}),j=(0,a.Gp)((e,n)=>{let t=m();return c.createElement(a.m$.tr,{role:"row",...e,ref:n,__css:t.tr})}),_=(0,a.Gp)(({isNumeric:e,...n},t)=>{let r=m();return c.createElement(a.m$.td,{role:"gridcell",...n,ref:t,__css:r.td,"data-is-numeric":e})}),w=t(63679),b=t(9008),k=t.n(b),y=t(41664),g=t.n(y),E=t(6448);function z(e){var n,t,r,i;return"columnDesc"in e?"".concat(null===(n=e.columnDesc)||void 0===n?void 0:n.name," (").concat(null===(r=e.columnDesc)||void 0===r?void 0:null===(t=r.columnType)||void 0===t?void 0:t.typeName,")"):"".concat(e.name," (").concat(null===(i=e.dataType)||void 0===i?void 0:i.typeName,")")}var C=t(49023),N=t(62568);let T=(0,w.ZP)(()=>t.e(171).then(t.t.bind(t,55171,23))),S={name:"Depends",width:1,content:e=>(0,r.jsx)(g(),{href:"/dependency_graph/?id=".concat(e.id),children:(0,r.jsx)(i.zx,{size:"sm","aria-label":"view dependents",colorScheme:"blue",variant:"link",children:"D"})})},D={name:"Primary Key",width:1,content:e=>e.pk.map(e=>e.columnIndex).map(n=>e.columns[n]).map(e=>z(e)).join(", ")},G={name:"Connector",width:3,content:e=>{var n;return null!==(n=e.withProperties.connector)&&void 0!==n?n:"unknown"}},$={name:"Connector",width:3,content:e=>{var n;return null!==(n=e.properties.connector)&&void 0!==n?n:"unknown"}},P=[S,{name:"Fragments",width:1,content:e=>(0,r.jsx)(g(),{href:"/fragment_graph/?id=".concat(e.id),children:(0,r.jsx)(i.zx,{size:"sm","aria-label":"view fragments",colorScheme:"blue",variant:"link",children:"F"})})}];function W(e,n,t){let{response:a}=(0,C.Z)(n),[o,s]=(0,N.r)(a),d=(0,r.jsx)(N.D,{modalData:o,onClose:()=>s(null)}),u=(0,r.jsxs)(l.xu,{p:3,children:[(0,r.jsx)(E.Z,{children:e}),(0,r.jsx)(p,{children:(0,r.jsxs)(h,{variant:"simple",size:"sm",maxWidth:"full",children:[(0,r.jsx)(x,{children:(0,r.jsxs)(j,{children:[(0,r.jsx)(v,{width:3,children:"Id"}),(0,r.jsx)(v,{width:5,children:"Name"}),(0,r.jsx)(v,{width:3,children:"Owner"}),t.map(e=>(0,r.jsx)(v,{width:e.width,children:e.name},e.name)),(0,r.jsx)(v,{children:"Visible Columns"})]})}),(0,r.jsx)(f,{children:null==a?void 0:a.map(e=>(0,r.jsxs)(j,{children:[(0,r.jsx)(_,{children:(0,r.jsx)(i.zx,{size:"sm","aria-label":"view catalog",colorScheme:"blue",variant:"link",onClick:()=>s(e.id),children:e.id})}),(0,r.jsx)(_,{children:e.name}),(0,r.jsx)(_,{children:e.owner}),t.map(n=>(0,r.jsx)(_,{children:n.content(e)},n.name)),(0,r.jsx)(_,{overflowWrap:"normal",children:e.columns.filter(e=>!("isHidden"in e)||!e.isHidden).map(e=>z(e)).join(", ")})]},e.id))})]})})]});return(0,r.jsxs)(c.Fragment,{children:[(0,r.jsx)(k(),{children:(0,r.jsx)("title",{children:e})}),d,u]})}},48001:function(e,n,t){"use strict";t.r(n),t.d(n,{default:function(){return l}});var r=t(7963),i=t(664);function l(){return(0,r.KB)("Materialized Views",i.BA,[...r.sW,r.vP])}}},function(e){e.O(0,[662,679,184,667,970,661,888,774,179],function(){return e(e.s=92560)}),_N_E=e.O()}]);