var mM=Object.defineProperty,fM=Object.defineProperties;var hM=Object.getOwnPropertyDescriptors;var jy=Object.getOwnPropertySymbols;var pM=Object.prototype.hasOwnProperty,gM=Object.prototype.propertyIsEnumerable;var Vy=(t,n,e)=>n in t?mM(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,b=(t,n)=>{for(var e in n||={})pM.call(n,e)&&Vy(t,e,n[e]);if(jy)for(var e of jy(n))gM.call(n,e)&&Vy(t,e,n[e]);return t},J=(t,n)=>fM(t,hM(n));var Ns=(t=>typeof require<"u"?require:typeof Proxy<"u"?new Proxy(t,{get:(n,e)=>(typeof require<"u"?require:n)[e]}):t)(function(t){if(typeof require<"u")return require.apply(this,arguments);throw Error('Dynamic require of "'+t+'" is not supported')});var tn=null,Md=!1,cp=1,_M=null,Dt=Symbol("SIGNAL");function re(t){let n=tn;return tn=t,n}function Td(){return tn}var Pr={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Fr(t){if(Md)throw new Error("");if(tn===null)return;tn.consumerOnSignalRead(t);let n=tn.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=tn.recomputing;if(i&&(e=n!==void 0?n.nextProducer:tn.producers,e!==void 0&&e.producer===t)){tn.producersTail=e,e.lastReadVersion=t.version;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===tn&&(!i||bM(r,tn)))return;let o=Ls(tn),s={producer:t,consumer:tn,nextProducer:e,prevConsumer:r,lastReadVersion:t.version,nextConsumer:void 0};tn.producersTail=s,n!==void 0?n.nextProducer=s:tn.producers=s,o&&$y(t,s)}function Hy(){cp++}function To(t){if(!(Ls(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===cp)){if(!t.producerMustRecompute(t)&&!Fs(t)){Ps(t);return}t.producerRecomputeValue(t),Ps(t)}}function lp(t){if(t.consumers===void 0)return;let n=Md;Md=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||vM(i)}}finally{Md=n}}function dp(){return tn?.consumerAllowSignalWrites!==!1}function vM(t){t.dirty=!0,lp(t),t.consumerMarkedDirty?.(t)}function Ps(t){t.dirty=!1,t.lastCleanEpoch=cp}function cr(t){return t&&zy(t),re(t)}function zy(t){t.producersTail=void 0,t.recomputing=!0}function Lr(t,n){re(n),t&&Uy(t)}function Uy(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(Ls(t))do e=up(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function Fs(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(To(e),i!==e.version))return!0}return!1}function Br(t){if(Ls(t)){let n=t.producers;for(;n!==void 0;)n=up(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function $y(t,n){let e=t.consumersTail,i=Ls(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)$y(r.producer,r)}function up(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!Ls(n)){let o=n.producers;for(;o!==void 0;)o=up(o)}return e}function Ls(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function lc(t){_M?.(t)}function bM(t,n){let e=n.producersTail;if(e!==void 0){let i=n.producers;do{if(i===t)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function dc(t,n){return Object.is(t,n)}function uc(t,n){let e=Object.create(yM);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(To(e),Fr(e),e.value===Ti)throw e.error;return e.value};return i[Dt]=e,lc(e),i}var ko=Symbol("UNSET"),Mo=Symbol("COMPUTING"),Ti=Symbol("ERRORED"),yM=J(b({},Pr),{value:ko,dirty:!0,error:null,equal:dc,kind:"computed",producerMustRecompute(t){return t.value===ko||t.value===Mo},producerRecomputeValue(t){if(t.value===Mo)throw new Error("");let n=t.value;t.value=Mo;let e=cr(t),i,r=!1;try{i=t.computation(),re(null),r=n!==ko&&n!==Ti&&i!==Ti&&t.equal(n,i)}catch(o){i=Ti,t.error=o}finally{Lr(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function CM(){throw new Error}var Gy=CM;function Wy(t){Gy(t)}function mp(t){Gy=t}var xM=null;function fp(t,n){let e=Object.create(mc);e.value=t,n!==void 0&&(e.equal=n);let i=()=>qy(e);return i[Dt]=e,lc(e),[i,s=>Ao(e,s),s=>Ad(e,s)]}function qy(t){return Fr(t),t.value}function Ao(t,n){dp()||Wy(t),t.equal(t.value,n)||(t.value=n,wM(t))}function Ad(t,n){dp()||Wy(t),Ao(t,n(t.value))}var mc=J(b({},Pr),{equal:dc,value:void 0,kind:"signal"});function wM(t){t.version++,Hy(),lp(t),xM?.(t)}var hp=J(b({},Pr),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function pp(t){if(t.dirty=!1,t.version>0&&!Fs(t))return;t.version++;let n=cr(t);try{t.cleanup(),t.fn()}finally{Lr(t,n)}}function fe(t){return typeof t=="function"}function Bs(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Rd=Bs(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function Ro(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var ve=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(fe(i))try{i()}catch(o){n=o instanceof Rd?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{Yy(o)}catch(s){n=n??[],s instanceof Rd?n=[...n,...s.errors]:n.push(s)}}if(n)throw new Rd(n)}}add(n){var e;if(n&&n!==this)if(this.closed)Yy(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&Ro(e,n)}remove(n){let{_finalizers:e}=this;e&&Ro(e,n),n instanceof t&&n._removeParent(this)}};ve.EMPTY=(()=>{let t=new ve;return t.closed=!0,t})();var gp=ve.EMPTY;function Od(t){return t instanceof ve||t&&"closed"in t&&fe(t.remove)&&fe(t.add)&&fe(t.unsubscribe)}function Yy(t){fe(t)?t():t.unsubscribe()}var si={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var js={setTimeout(t,n,...e){let{delegate:i}=js;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=js;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function Nd(t){js.setTimeout(()=>{let{onUnhandledError:n}=si;if(n)n(t);else throw t})}function Oo(){}var Zy=_p("C",void 0,void 0);function Qy(t){return _p("E",void 0,t)}function Xy(t){return _p("N",t,void 0)}function _p(t,n,e){return{kind:t,value:n,error:e}}var No=null;function Vs(t){if(si.useDeprecatedSynchronousErrorHandling){let n=!No;if(n&&(No={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=No;if(No=null,e)throw i}}else t()}function Ky(t){si.useDeprecatedSynchronousErrorHandling&&No&&(No.errorThrown=!0,No.error=t)}var Po=class extends ve{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Od(n)&&n.add(this)):this.destination=SM}static create(n,e,i){return new ai(n,e,i)}next(n){this.isStopped?bp(Xy(n),this):this._next(n)}error(n){this.isStopped?bp(Qy(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?bp(Zy,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},DM=Function.prototype.bind;function vp(t,n){return DM.call(t,n)}var yp=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){Pd(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){Pd(i)}else Pd(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){Pd(e)}}},ai=class extends Po{constructor(n,e,i){super();let r;if(fe(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&si.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&vp(n.next,o),error:n.error&&vp(n.error,o),complete:n.complete&&vp(n.complete,o)}):r=n}this.destination=new yp(r)}};function Pd(t){si.useDeprecatedSynchronousErrorHandling?Ky(t):Nd(t)}function EM(t){throw t}function bp(t,n){let{onStoppedNotification:e}=si;e&&js.setTimeout(()=>e(t,n))}var SM={closed:!0,next:Oo,error:EM,complete:Oo};var Hs=typeof Symbol=="function"&&Symbol.observable||"@@observable";function nn(t){return t}function Cp(...t){return xp(t)}function xp(t){return t.length===0?nn:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var ce=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=kM(e)?e:new ai(e,i,r);return Vs(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=Jy(i),new i((r,o)=>{let s=new ai({next:a=>{try{e(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[Hs](){return this}pipe(...e){return xp(e)(this)}toPromise(e){return e=Jy(e),new e((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return t.create=n=>new t(n),t})();function Jy(t){var n;return(n=t??si.Promise)!==null&&n!==void 0?n:Promise}function IM(t){return t&&fe(t.next)&&fe(t.error)&&fe(t.complete)}function kM(t){return t&&t instanceof Po||IM(t)&&Od(t)}function MM(t){return fe(t?.lift)}function de(t){return n=>{if(MM(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function he(t,n,e,i,r){return new wp(t,n,e,i,r)}var wp=class extends Po{constructor(n,e,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(c){n.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var e0=Bs(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var I=(()=>{class t extends ce{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new Fd(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new e0}next(e){Vs(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){Vs(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){Vs(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?gp:(this.currentObservers=null,o.push(e),new ve(()=>{this.currentObservers=null,Ro(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new ce;return e.source=this,e}}return t.create=(n,e)=>new Fd(n,e),t})(),Fd=class extends I{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:gp}};var Bt=class extends I{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var fc={now(){return(fc.delegate||Date).now()},delegate:void 0};var Ai=class extends I{constructor(n=1/0,e=1/0,i=fc){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let c=1;c<i.length&&i[c]<=s;c+=2)a=c;a&&i.splice(0,a+1)}}};var Ld=class extends ve{constructor(n,e){super()}schedule(n,e=0){return this}};var hc={setInterval(t,n,...e){let{delegate:i}=hc;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=hc;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var Bd=class extends Ld{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return hc.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&hc.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,Ro(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var zs=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};zs.now=fc.now;var jd=class extends zs{constructor(n,e=zs.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var Fo=new jd(Bd),t0=Fo;var Et=new ce(t=>t.complete());function Vd(t){return t&&fe(t.schedule)}function Dp(t){return t[t.length-1]}function Hd(t){return fe(Dp(t))?t.pop():void 0}function Ri(t){return Vd(Dp(t))?t.pop():void 0}function n0(t,n){return typeof Dp(t)=="number"?t.pop():n}function r0(t,n,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(u){try{l(i.next(u))}catch(m){s(m)}}function c(u){try{l(i.throw(u))}catch(m){s(m)}}function l(u){u.done?o(u.value):r(u.value).then(a,c)}l((i=i.apply(t,n||[])).next())})}function i0(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Lo(t){return this instanceof Lo?(this.v=t,this):new Lo(t)}function o0(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(_){return function(x){return Promise.resolve(x).then(_,m)}}function a(_,x){i[_]&&(r[_]=function(O){return new Promise(function(B,q){o.push([_,O,B,q])>1||c(_,O)})},x&&(r[_]=x(r[_])))}function c(_,x){try{l(i[_](x))}catch(O){g(o[0][3],O)}}function l(_){_.value instanceof Lo?Promise.resolve(_.value.v).then(u,m):g(o[0][2],_)}function u(_){c("next",_)}function m(_){c("throw",_)}function g(_,x){_(x),o.shift(),o.length&&c(o[0][0],o[0][1])}}function s0(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof i0=="function"?i0(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(s){return new Promise(function(a,c){s=t[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var zd=t=>t&&typeof t.length=="number"&&typeof t!="function";function Ud(t){return fe(t?.then)}function $d(t){return fe(t[Hs])}function Gd(t){return Symbol.asyncIterator&&fe(t?.[Symbol.asyncIterator])}function Wd(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function TM(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var qd=TM();function Yd(t){return fe(t?.[qd])}function Zd(t){return o0(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield Lo(e.read());if(r)return yield Lo(void 0);yield yield Lo(i)}}finally{e.releaseLock()}})}function Qd(t){return fe(t?.getReader)}function He(t){if(t instanceof ce)return t;if(t!=null){if($d(t))return AM(t);if(zd(t))return RM(t);if(Ud(t))return OM(t);if(Gd(t))return a0(t);if(Yd(t))return NM(t);if(Qd(t))return PM(t)}throw Wd(t)}function AM(t){return new ce(n=>{let e=t[Hs]();if(fe(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function RM(t){return new ce(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function OM(t){return new ce(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,Nd)})}function NM(t){return new ce(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function a0(t){return new ce(n=>{FM(t,n).catch(e=>n.error(e))})}function PM(t){return a0(Zd(t))}function FM(t,n){var e,i,r,o;return r0(this,void 0,void 0,function*(){try{for(e=s0(t);i=yield e.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function wn(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function Xd(t,n=0){return de((e,i)=>{e.subscribe(he(i,r=>wn(i,t,()=>i.next(r),n),()=>wn(i,t,()=>i.complete(),n),r=>wn(i,t,()=>i.error(r),n)))})}function Kd(t,n=0){return de((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function c0(t,n){return He(t).pipe(Kd(n),Xd(n))}function l0(t,n){return He(t).pipe(Kd(n),Xd(n))}function d0(t,n){return new ce(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function u0(t,n){return new ce(e=>{let i;return wn(e,n,()=>{i=t[qd](),wn(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>fe(i?.return)&&i.return()})}function Jd(t,n){if(!t)throw new Error("Iterable cannot be null");return new ce(e=>{wn(e,n,()=>{let i=t[Symbol.asyncIterator]();wn(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function m0(t,n){return Jd(Zd(t),n)}function f0(t,n){if(t!=null){if($d(t))return c0(t,n);if(zd(t))return d0(t,n);if(Ud(t))return l0(t,n);if(Gd(t))return Jd(t,n);if(Yd(t))return u0(t,n);if(Qd(t))return m0(t,n)}throw Wd(t)}function rt(t,n){return n?f0(t,n):He(t)}function Q(...t){let n=Ri(t);return rt(t,n)}function pc(t,n){let e=fe(t)?t:()=>t,i=r=>r.error(e());return new ce(n?r=>n.schedule(i,0,r):i)}function gc(t){return!!t&&(t instanceof ce||fe(t.lift)&&fe(t.subscribe))}var ci=Bs(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function Ep(t,n){let e=typeof n=="object";return new Promise((i,r)=>{let o=!1,s;t.subscribe({next:a=>{s=a,o=!0},error:r,complete:()=>{o?i(s):e?i(n.defaultValue):r(new ci)}})})}function Bo(t,n){let e=typeof n=="object";return new Promise((i,r)=>{let o=new ai({next:s=>{i(s),o.unsubscribe()},error:r,complete:()=>{e?i(n.defaultValue):r(new ci)}});t.subscribe(o)})}function h0(t){return t instanceof Date&&!isNaN(t)}function ee(t,n){return de((e,i)=>{let r=0;e.subscribe(he(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:LM}=Array;function BM(t,n){return LM(n)?t(...n):t(n)}function eu(t){return ee(n=>BM(t,n))}var{isArray:jM}=Array,{getPrototypeOf:VM,prototype:HM,keys:zM}=Object;function tu(t){if(t.length===1){let n=t[0];if(jM(n))return{args:n,keys:null};if(UM(n)){let e=zM(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function UM(t){return t&&typeof t=="object"&&VM(t)===HM}function nu(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function _c(...t){let n=Ri(t),e=Hd(t),{args:i,keys:r}=tu(t);if(i.length===0)return rt([],n);let o=new ce($M(i,n,r?s=>nu(r,s):nn));return e?o.pipe(eu(e)):o}function $M(t,n,e=nn){return i=>{p0(n,()=>{let{length:r}=t,o=new Array(r),s=r,a=r;for(let c=0;c<r;c++)p0(n,()=>{let l=rt(t[c],n),u=!1;l.subscribe(he(i,m=>{o[c]=m,u||(u=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function p0(t,n,e){t?wn(e,t,n):n()}function g0(t,n,e,i,r,o,s,a){let c=[],l=0,u=0,m=!1,g=()=>{m&&!c.length&&!l&&n.complete()},_=O=>l<i?x(O):c.push(O),x=O=>{o&&n.next(O),l++;let B=!1;He(e(O,u++)).subscribe(he(n,q=>{r?.(q),o?_(q):n.next(q)},()=>{B=!0},void 0,()=>{if(B)try{for(l--;c.length&&l<i;){let q=c.shift();s?wn(n,s,()=>x(q)):x(q)}g()}catch(q){n.error(q)}}))};return t.subscribe(he(n,_,()=>{m=!0,g()})),()=>{a?.()}}function $t(t,n,e=1/0){return fe(n)?$t((i,r)=>ee((o,s)=>n(i,o,r,s))(He(t(i,r))),e):(typeof n=="number"&&(e=n),de((i,r)=>g0(i,r,t,e)))}function jr(t=1/0){return $t(nn,t)}function _0(){return jr(1)}function Oi(...t){return _0()(rt(t,Ri(t)))}function li(t){return new ce(n=>{He(t()).subscribe(n)})}function jo(...t){let n=Hd(t),{args:e,keys:i}=tu(t),r=new ce(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let u=0;u<s;u++){let m=!1;He(e[u]).subscribe(he(o,g=>{m||(m=!0,l--),a[u]=g},()=>c--,void 0,()=>{(!c||!m)&&(l||o.next(i?nu(i,a):a),o.complete())}))}});return n?r.pipe(eu(n)):r}function iu(t=0,n,e=t0){let i=-1;return n!=null&&(Vd(n)?e=n:i=n),new ce(r=>{let o=h0(t)?+t-e.now():t;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function fn(...t){let n=Ri(t),e=n0(t,1/0),i=t;return i.length?i.length===1?He(i[0]):jr(e)(rt(i,n)):Et}function xe(t,n){return de((e,i)=>{let r=0;e.subscribe(he(i,o=>t.call(n,o,r++)&&i.next(o)))})}function v0(t){return de((n,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let l=r;r=null,e.next(l)}s&&e.complete()},c=()=>{o=null,s&&e.complete()};n.subscribe(he(e,l=>{i=!0,r=l,o||He(t(l)).subscribe(o=he(e,a,c))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function ru(t,n=Fo){return v0(()=>iu(t,n))}function di(t){return de((n,e)=>{let i=null,r=!1,o;i=n.subscribe(he(e,void 0,void 0,s=>{o=He(t(s,di(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function Vo(t,n){return fe(n)?$t(t,n,1):$t(t,1)}function Ho(t,n=Fo){return de((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let l=o;o=null,i.next(l)}};function c(){let l=s+t,u=n.now();if(u<l){r=this.schedule(void 0,l-u),i.add(r);return}a()}e.subscribe(he(i,l=>{o=l,s=n.now(),r||(r=n.schedule(c,t),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function b0(t){return de((n,e)=>{let i=!1;n.subscribe(he(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function Ke(t){return t<=0?()=>Et:de((n,e)=>{let i=0;n.subscribe(he(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function y0(){return de((t,n)=>{t.subscribe(he(n,Oo))})}function C0(t){return ee(()=>t)}function Sp(t,n){return n?e=>Oi(n.pipe(Ke(1),y0()),e.pipe(Sp(t))):$t((e,i)=>He(t(e,i)).pipe(Ke(1),C0(e)))}function Ip(t,n=Fo){let e=iu(t,n);return Sp(()=>e)}function ou(t,n=nn){return t=t??GM,de((e,i)=>{let r,o=!0;e.subscribe(he(i,s=>{let a=n(s);(o||!t(r,a))&&(o=!1,r=a,i.next(s))}))})}function GM(t,n){return t===n}function x0(t=WM){return de((n,e)=>{let i=!1;n.subscribe(he(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function WM(){return new ci}function Ni(t){return de((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function lr(t,n){let e=arguments.length>=2;return i=>i.pipe(t?xe((r,o)=>t(r,o,i)):nn,Ke(1),e?b0(n):x0(()=>new ci))}function su(t){return t<=0?()=>Et:de((n,e)=>{let i=[];n.subscribe(he(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function au(){return de((t,n)=>{let e,i=!1;t.subscribe(he(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function vc(t={}){let{connector:n=()=>new I,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let s,a,c,l=0,u=!1,m=!1,g=()=>{a?.unsubscribe(),a=void 0},_=()=>{g(),s=c=void 0,u=m=!1},x=()=>{let O=s;_(),O?.unsubscribe()};return de((O,B)=>{l++,!m&&!u&&g();let q=c=c??n();B.add(()=>{l--,l===0&&!m&&!u&&(a=kp(x,r))}),q.subscribe(B),!s&&l>0&&(s=new ai({next:be=>q.next(be),error:be=>{m=!0,g(),a=kp(_,e,be),q.error(be)},complete:()=>{u=!0,g(),a=kp(_,i),q.complete()}}),He(O).subscribe(s))})(o)}}function kp(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new ai({next:()=>{i.unsubscribe(),t()}});return He(n(...e)).subscribe(i)}function cu(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,vc({connector:()=>new Ai(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function bc(t){return xe((n,e)=>t<=e)}function Je(...t){let n=Ri(t);return de((e,i)=>{(n?Oi(t,e,n):Oi(t,e)).subscribe(i)})}function _t(t,n){return de((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(he(i,c=>{r?.unsubscribe();let l=0,u=o++;He(t(c,u)).subscribe(r=he(i,m=>i.next(n?n(c,m,u,l++):m),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function pe(t){return de((n,e)=>{He(t).subscribe(he(e,()=>e.complete(),Oo)),!e.closed&&n.subscribe(e)})}function Mp(t,n=!1){return de((e,i)=>{let r=0;e.subscribe(he(i,o=>{let s=t(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function ct(t,n,e){let i=fe(t)||n||e?{next:t,error:n,complete:e}:t;return i?de((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(he(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):nn}var Tp;function lu(){return Tp}function Pi(t){let n=Tp;return Tp=t,n}var w0=Symbol("NotFound");function Us(t){return t===w0||t?.name==="\u0275NotFound"}function Ap(t,n,e){let i=Object.create(qM);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(To(i),Fr(i),i.value===Ti)throw i.error;return i.value};return o[Dt]=i,lc(i),o}function D0(t,n){To(t),Ao(t,n),Ps(t)}function E0(t,n){if(To(t),t.value===Ti)throw t.error;Ad(t,n),Ps(t)}var qM=J(b({},Pr),{value:ko,dirty:!0,error:null,equal:dc,kind:"linkedSignal",producerMustRecompute(t){return t.value===ko||t.value===Mo},producerRecomputeValue(t){if(t.value===Mo)throw new Error("");let n=t.value;t.value=Mo;let e=cr(t),i,r=!1;try{let o=t.source(),s=n!==ko&&n!==Ti,a=s?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,a),t.sourceValue=o,re(null),r=s&&i!==Ti&&t.equal(n,i)}catch(o){i=Ti,t.error=o}finally{Lr(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function S0(t){let n=re(null);try{return t()}finally{re(n)}}var gu="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",A=class extends Error{code;constructor(n,e){super(Li(n,e)),this.code=n}};function YM(t){return`NG0${Math.abs(t)}`}function Li(t,n){return`${YM(t)}${n?": "+n:""}`}var $n=globalThis;function Fe(t){for(let n in t)if(t[n]===Fe)return n;throw Error("")}function A0(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function Sc(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(Sc).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function _u(t,n){return t?n?`${t} ${n}`:t:n||""}var ZM=Fe({__forward_ref__:Fe});function Rt(t){return t.__forward_ref__=Rt,t}function Qt(t){return $p(t)?t():t}function $p(t){return typeof t=="function"&&t.hasOwnProperty(ZM)&&t.__forward_ref__===Rt}function C(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function G(t){return{providers:t.providers||[],imports:t.imports||[]}}function Ic(t){return QM(t,vu)}function Gp(t){return Ic(t)!==null}function QM(t,n){return t.hasOwnProperty(n)&&t[n]||null}function XM(t){let n=t?.[vu]??null;return n||null}function Op(t){return t&&t.hasOwnProperty(uu)?t[uu]:null}var vu=Fe({\u0275prov:Fe}),uu=Fe({\u0275inj:Fe}),y=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=C({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Wp(t){return t&&!!t.\u0275providers}var qp=Fe({\u0275cmp:Fe}),Yp=Fe({\u0275dir:Fe}),Zp=Fe({\u0275pipe:Fe}),Qp=Fe({\u0275mod:Fe}),Cc=Fe({\u0275fac:Fe}),Wo=Fe({__NG_ELEMENT_ID__:Fe}),I0=Fe({__NG_ENV_ID__:Fe});function Xp(t){return bu(t,"@NgModule"),t[Qp]||null}function ur(t){return bu(t,"@Component"),t[qp]||null}function Kp(t){return bu(t,"@Directive"),t[Yp]||null}function R0(t){return bu(t,"@Pipe"),t[Zp]||null}function bu(t,n){if(t==null)throw new A(-919,!1)}function qo(t){return typeof t=="string"?t:t==null?"":String(t)}var O0=Fe({ngErrorCode:Fe}),KM=Fe({ngErrorMessage:Fe}),JM=Fe({ngTokenPath:Fe});function Jp(t,n){return N0("",-200,n)}function yu(t,n){throw new A(-201,!1)}function N0(t,n,e){let i=new A(n,t);return i[O0]=n,i[KM]=t,e&&(i[JM]=e),i}function eT(t){return t[O0]}var Np;function P0(){return Np}function hn(t){let n=Np;return Np=t,n}function eg(t,n,e){let i=Ic(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;yu(t,"")}var tT={},zo=tT,nT="__NG_DI_FLAG__",Pp=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=Uo(e)||0;try{return this.injector.get(n,i&8?null:zo,i)}catch(r){if(Us(r))return r;throw r}}};function iT(t,n=0){let e=lu();if(e===void 0)throw new A(-203,!1);if(e===null)return eg(t,void 0,n);{let i=rT(n),r=e.retrieve(t,i);if(Us(r)){if(i.optional)return null;throw r}return r}}function te(t,n=0){return(P0()||iT)(Qt(t),n)}function d(t,n){return te(t,Uo(n))}function Uo(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function rT(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function Fp(t){let n=[];for(let e=0;e<t.length;e++){let i=Qt(t[e]);if(Array.isArray(i)){if(i.length===0)throw new A(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=oT(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}n.push(te(r,o))}else n.push(te(i))}return n}function oT(t){return t[nT]}function Vr(t,n){let e=t.hasOwnProperty(Cc);return e?t[Cc]:null}function F0(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function L0(t){return t.flat(Number.POSITIVE_INFINITY)}function Cu(t,n){t.forEach(e=>Array.isArray(e)?Cu(e,n):n(e))}function tg(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function kc(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function B0(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function j0(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function xu(t,n,e){let i=Gs(t,n);return i>=0?t[i|1]=e:(i=~i,j0(t,i,n,e)),i}function wu(t,n){let e=Gs(t,n);if(e>=0)return t[e|1]}function Gs(t,n){return sT(t,n,1)}function sT(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=t[o<<e];if(n===s)return o<<e;s>n?r=o:i=o+1}return~(r<<e)}var Ur={},rn=[],$r=new y(""),ng=new y("",-1),ig=new y(""),xc=class{get(n,e=zo){if(e===zo){let r=N0("",-201);throw r.name="\u0275NotFound",r}return e}};function Bi(t){return{\u0275providers:t}}function V0(t){return Bi([{provide:$r,multi:!0,useValue:t}])}function H0(...t){return{\u0275providers:rg(!0,t),\u0275fromNgModule:!0}}function rg(t,...n){let e=[],i=new Set,r,o=s=>{e.push(s)};return Cu(n,s=>{let a=s;mu(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&z0(r,o),e}function z0(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];og(r,o=>{n(o,i)})}}function mu(t,n,e,i){if(t=Qt(t),!t)return!1;let r=null,o=Op(t),s=!o&&ur(t);if(!o&&!s){let c=t.ngModule;if(o=Op(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=t}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)mu(l,n,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;Cu(o.imports,u=>{mu(u,n,e,i)&&(l||=[],l.push(u))}),l!==void 0&&z0(l,n)}if(!a){let l=Vr(r)||(()=>new r);n({provide:r,useFactory:l,deps:rn},r),n({provide:ig,useValue:r,multi:!0},r),n({provide:$r,useValue:()=>te(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=t;og(c,u=>{n(u,l)})}}else return!1;return r!==t&&t.providers!==void 0}function og(t,n){for(let e of t)Wp(e)&&(e=e.\u0275providers),Array.isArray(e)?og(e,n):n(e)}var aT=Fe({provide:String,useValue:Fe});function U0(t){return t!==null&&typeof t=="object"&&aT in t}function cT(t){return!!(t&&t.useExisting)}function lT(t){return!!(t&&t.useFactory)}function $o(t){return typeof t=="function"}function $0(t){return!!t.useClass}var Mc=new y(""),du={},k0={},Rp;function Ws(){return Rp===void 0&&(Rp=new xc),Rp}var ze=class{},Go=class extends ze{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,Bp(n,s=>this.processProvider(s)),this.records.set(ng,$s(void 0,this)),r.has("environment")&&this.records.set(ze,$s(void 0,this));let o=this.records.get(Mc);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(ig,rn,{self:!0}))}retrieve(n,e){let i=Uo(e)||0;try{return this.get(n,zo,i)}catch(r){if(Us(r))return r;throw r}}destroy(){yc(this),this._destroyed=!0;let n=re(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),re(n)}}onDestroy(n){return yc(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){yc(this);let e=Pi(this),i=hn(void 0),r;try{return n()}finally{Pi(e),hn(i)}}get(n,e=zo,i){if(yc(this),n.hasOwnProperty(I0))return n[I0](this);let r=Uo(i),o,s=Pi(this),a=hn(void 0);try{if(!(r&4)){let l=this.records.get(n);if(l===void 0){let u=hT(n)&&Ic(n);u&&this.injectableDefInScope(u)?l=$s(Lp(n),du):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,r)}let c=r&2?Ws():this.parent;return e=r&8&&e===zo?null:e,c.get(n,e)}catch(c){let l=eT(c);throw l===-200||l===-201?new A(l,null):c}finally{hn(a),Pi(s)}}resolveInjectorInitializers(){let n=re(null),e=Pi(this),i=hn(void 0),r;try{let o=this.get($r,rn,{self:!0});for(let s of o)s()}finally{Pi(e),hn(i),re(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=Qt(n);let e=$o(n)?n:Qt(n&&n.provide),i=uT(n);if(!$o(n)&&n.multi===!0){let r=this.records.get(e);r||(r=$s(void 0,du,!0),r.factory=()=>Fp(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=re(null);try{if(e.value===k0)throw Jp("");return e.value===du&&(e.value=k0,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&fT(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{re(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=Qt(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function Lp(t){let n=Ic(t),e=n!==null?n.factory:Vr(t);if(e!==null)return e;if(t instanceof y)throw new A(-204,!1);if(t instanceof Function)return dT(t);throw new A(-204,!1)}function dT(t){if(t.length>0)throw new A(-204,!1);let e=XM(t);return e!==null?()=>e.factory(t):()=>new t}function uT(t){if(U0(t))return $s(void 0,t.useValue);{let n=sg(t);return $s(n,du)}}function sg(t,n,e){let i;if($o(t)){let r=Qt(t);return Vr(r)||Lp(r)}else if(U0(t))i=()=>Qt(t.useValue);else if(lT(t))i=()=>t.useFactory(...Fp(t.deps||[]));else if(cT(t))i=(r,o)=>te(Qt(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=Qt(t&&(t.useClass||t.provide));if(mT(t))i=()=>new r(...Fp(t.deps));else return Vr(r)||Lp(r)}return i}function yc(t){if(t.destroyed)throw new A(-205,!1)}function $s(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function mT(t){return!!t.deps}function fT(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function hT(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function Bp(t,n){for(let e of t)Array.isArray(e)?Bp(e,n):e&&Wp(e)?Bp(e.\u0275providers,n):n(e)}function Gt(t,n){let e;t instanceof Go?(yc(t),e=t):e=new Pp(t);let i,r=Pi(e),o=hn(void 0);try{return n()}finally{Pi(r),hn(o)}}function G0(){return P0()!==void 0||lu()!=null}var ui=0,oe=1,ae=2,jt=3,Gn=4,pn=5,Yo=6,qs=7,St=8,mr=9,mi=10,Ue=11,Ys=12,ag=13,Zo=14,gn=15,Gr=16,Qo=17,ji=18,fr=19,cg=20,dr=21,Du=22,Hr=23,On=24,Xo=25,Wr=26,ot=27,W0=1,lg=6,qr=7,Tc=8,Ko=9,vt=10;function hr(t){return Array.isArray(t)&&typeof t[W0]=="object"}function fi(t){return Array.isArray(t)&&t[W0]===!0}function dg(t){return(t.flags&4)!==0}function Vi(t){return t.componentOffset>-1}function Zs(t){return(t.flags&1)===1}function Hi(t){return!!t.template}function Qs(t){return(t[ae]&512)!==0}function Jo(t){return(t[ae]&256)===256}var ug="svg",q0="math";function Wn(t){for(;Array.isArray(t);)t=t[ui];return t}function mg(t,n){return Wn(n[t])}function qn(t,n){return Wn(n[t.index])}function Eu(t,n){return t.data[n]}function Su(t,n){return t[n]}function fg(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function Yn(t,n){let e=n[t];return hr(e)?e:e[ui]}function Y0(t){return(t[ae]&4)===4}function Iu(t){return(t[ae]&128)===128}function Z0(t){return fi(t[jt])}function Nn(t,n){return n==null?null:t[n]}function hg(t){t[Qo]=0}function pg(t){t[ae]&1024||(t[ae]|=1024,Iu(t)&&es(t))}function Q0(t,n){for(;t>0;)n=n[Zo],t--;return n}function Ac(t){return!!(t[ae]&9216||t[On]?.dirty)}function ku(t){t[mi].changeDetectionScheduler?.notify(8),t[ae]&64&&(t[ae]|=1024),Ac(t)&&es(t)}function es(t){t[mi].changeDetectionScheduler?.notify(0);let n=zr(t);for(;n!==null&&!(n[ae]&8192||(n[ae]|=8192,!Iu(n)));)n=zr(n)}function gg(t,n){if(Jo(t))throw new A(911,!1);t[dr]===null&&(t[dr]=[]),t[dr].push(n)}function X0(t,n){if(t[dr]===null)return;let e=t[dr].indexOf(n);e!==-1&&t[dr].splice(e,1)}function zr(t){let n=t[jt];return fi(n)?n[jt]:n}function _g(t){return t[qs]??=[]}function vg(t){return t.cleanup??=[]}function K0(t,n,e,i){let r=_g(n);r.push(e),t.firstCreatePass&&vg(t).push(i,r.length-1)}var ge={lFrame:dC(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var jp=!1;function J0(){return ge.lFrame.elementDepthCount}function eC(){ge.lFrame.elementDepthCount++}function bg(){ge.lFrame.elementDepthCount--}function Mu(){return ge.bindingsEnabled}function yg(){return ge.skipHydrationRootTNode!==null}function Cg(t){return ge.skipHydrationRootTNode===t}function xg(){ge.skipHydrationRootTNode=null}function ne(){return ge.lFrame.lView}function Ze(){return ge.lFrame.tView}function Me(t){return ge.lFrame.contextLView=t,t[St]}function Te(t){return ge.lFrame.contextLView=null,t}function Vt(){let t=wg();for(;t!==null&&t.type===64;)t=t.parent;return t}function wg(){return ge.lFrame.currentTNode}function tC(){let t=ge.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Xs(t,n){let e=ge.lFrame;e.currentTNode=t,e.isParent=n}function Dg(){return ge.lFrame.isParent}function Eg(){ge.lFrame.isParent=!1}function nC(){return ge.lFrame.contextLView}function Sg(){return jp}function wc(t){let n=jp;return jp=t,n}function Ks(){let t=ge.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function iC(){return ge.lFrame.bindingIndex}function rC(t){return ge.lFrame.bindingIndex=t}function zi(){return ge.lFrame.bindingIndex++}function Tu(t){let n=ge.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function oC(){return ge.lFrame.inI18n}function sC(t,n){let e=ge.lFrame;e.bindingIndex=e.bindingRootIndex=t,Au(n)}function aC(){return ge.lFrame.currentDirectiveIndex}function Au(t){ge.lFrame.currentDirectiveIndex=t}function cC(t){let n=ge.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function Ru(){return ge.lFrame.currentQueryIndex}function Rc(t){ge.lFrame.currentQueryIndex=t}function pT(t){let n=t[oe];return n.type===2?n.declTNode:n.type===1?t[pn]:null}function Ig(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=pT(o),r===null||(o=o[Zo],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=ge.lFrame=lC();return i.currentTNode=n,i.lView=t,!0}function Ou(t){let n=lC(),e=t[oe];ge.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function lC(){let t=ge.lFrame,n=t===null?null:t.child;return n===null?dC(t):n}function dC(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function uC(){let t=ge.lFrame;return ge.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var kg=uC;function Nu(){let t=uC();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function mC(t){return(ge.lFrame.contextLView=Q0(t,ge.lFrame.contextLView))[St]}function Ui(){return ge.lFrame.selectedIndex}function Yr(t){ge.lFrame.selectedIndex=t}function Js(){let t=ge.lFrame;return Eu(t.tView,t.selectedIndex)}function Wt(){ge.lFrame.currentNamespace=ug}function $i(){gT()}function gT(){ge.lFrame.currentNamespace=null}function fC(){return ge.lFrame.currentNamespace}var hC=!0;function Pu(){return hC}function Oc(t){hC=t}function Vp(t,n=null,e=null,i){let r=Mg(t,n,e,i);return r.resolveInjectorInitializers(),r}function Mg(t,n=null,e=null,i,r=new Set){let o=[e||rn,H0(t)],s;return new Go(o,n||Ws(),s||null,r)}var X=class t{static THROW_IF_NOT_FOUND=zo;static NULL=new xc;static create(n,e){if(Array.isArray(n))return Vp({name:""},e,n,"");{let i=n.name??"";return Vp({name:i},n.parent,n.providers,i)}}static \u0275prov=C({token:t,providedIn:"any",factory:()=>te(ng)});static __NG_ELEMENT_ID__=-1},K=new y(""),sn=(()=>{class t{static __NG_ELEMENT_ID__=_T;static __NG_ENV_ID__=e=>e}return t})(),fu=class extends sn{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Jo(this._lView)}onDestroy(n){let e=this._lView;return gg(e,n),()=>X0(e,n)}};function _T(){return new fu(ne())}var pC=!1,gC=new y(""),pr=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Bt(!1);debugTaskTracker=d(gC,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new ce(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=C({token:t,providedIn:"root",factory:()=>new t})}return t})(),Hp=class extends I{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,G0()&&(this.destroyRef=d(sn,{optional:!0})??void 0,this.pendingTasks=d(pr,{optional:!0})??void 0)}emit(n){let e=re(null);try{super.next(n)}finally{re(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),s=i;if(n&&typeof n=="object"){let c=n;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof ve&&n.add(a),a}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},H=Hp;function hu(...t){}function Tg(t){let n,e;function i(){t=hu;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function _C(t){return queueMicrotask(()=>t()),()=>{t=hu}}var Ag="isAngularZone",Dc=Ag+"_ID",vT=0,j=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new H(!1);onMicrotaskEmpty=new H(!1);onStable=new H(!1);onError=new H(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=pC}=n;if(typeof Zone>"u")throw new A(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,CT(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Ag)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new A(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new A(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,bT,hu,hu);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},bT={};function Rg(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function yT(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Tg(()=>{t.callbackScheduled=!1,zp(t),t.isCheckStableRunning=!0,Rg(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),zp(t)}function CT(t){let n=()=>{yT(t)},e=vT++;t._inner=t._inner.fork({name:"angular",properties:{[Ag]:!0,[Dc]:e,[Dc+e]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(xT(c))return i.invokeTask(o,s,a,c);try{return M0(t),i.invokeTask(o,s,a,c)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),T0(t)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return M0(t),i.invoke(o,s,a,c,l)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!wT(c)&&n(),T0(t)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,zp(t),Rg(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function zp(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function M0(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function T0(t){t._nesting--,Rg(t)}var Ec=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new H;onMicrotaskEmpty=new H;onStable=new H;onError=new H;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function xT(t){return vC(t,"__ignore_ng_zone__")}function wT(t){return vC(t,"__scheduler_tick__")}function vC(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var on=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Pn=new y("",{factory:()=>{let t=d(j),n=d(ze),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(on),e.handleError(i))})}}}),bC={provide:$r,useValue:()=>{let t=d(on,{optional:!0})},multi:!0},DT=new y("",{factory:()=>{let t=d(K).defaultView;if(!t)return;let n=d(Pn),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),d(sn).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function Og(){return Bi([V0(()=>{d(DT)})])}function N(t,n){let[e,i,r]=fp(t,n?.equal),o=e,s=o[Dt];return o.set=i,o.update=r,o.asReadonly=Fu.bind(o),o}function Fu(){let t=this[Dt];if(t.readonlyFn===void 0){let n=()=>this();n[Dt]=t,t.readonlyFn=n}return t.readonlyFn}var ea=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=ET}return t})();function ET(){return new ea(ne(),Vt())}var Fi=class{},Nc=new y("",{factory:()=>!0});var Ng=new y(""),Pc=(()=>{class t{internalPendingTasks=d(pr);scheduler=d(Fi);errorHandler=d(Pn);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();e().catch(this.errorHandler).finally(i)}static \u0275prov=C({token:t,providedIn:"root",factory:()=>new t})}return t})(),Lu=(()=>{class t{static \u0275prov=C({token:t,providedIn:"root",factory:()=>new Up})}return t})(),Up=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},pu=class{[Dt];constructor(n){this[Dt]=n}destroy(){this[Dt].destroy()}};function hi(t,n){let e=n?.injector??d(X),i=n?.manualCleanup!==!0?e.get(sn):null,r,o=e.get(ea,null,{optional:!0}),s=e.get(Fi);return o!==null?(r=kT(o.view,s,t),i instanceof fu&&i._lView===o.view&&(i=null)):r=MT(t,e.get(Lu),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new pu(r)}var yC=J(b({},hp),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=wc(!1);try{pp(this)}finally{wc(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=re(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],re(t)}}}),ST=J(b({},yC),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Br(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),IT=J(b({},yC),{consumerMarkedDirty(){this.view[ae]|=8192,es(this.view),this.notifier.notify(13)},destroy(){if(Br(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Hr]?.delete(this)}});function kT(t,n,e){let i=Object.create(IT);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=CC(i,e),t[Hr]??=new Set,t[Hr].add(i),i.consumerMarkedDirty(i),i}function MT(t,n,e){let i=Object.create(ST);return i.fn=CC(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function CC(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function Wc(t){return{toString:t}.toString()}function FT(t){return typeof t=="function"}function tx(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var qu=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}},$e=(()=>{let t=()=>nx;return t.ngInherit=!0,t})();function nx(t){return t.type.prototype.ngOnChanges&&(t.setInput=BT),LT}function LT(){let t=rx(this),n=t?.current;if(n){let e=t.previous;if(e===Ur)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function BT(t,n,e,i,r){let o=this.declaredInputs[i],s=rx(t)||jT(t,{previous:Ur,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new qu(l&&l.currentValue,e,c===Ur),tx(t,n,r,e)}var ix="__ngSimpleChanges__";function rx(t){return t[ix]||null}function jT(t,n){return t[ix]=n}var xC=[];var Le=function(t,n=null,e){for(let i=0;i<xC.length;i++){let r=xC[i];r(t,n,e)}},Ae=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(Ae||{});function VT(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=nx(n);(e.preOrderHooks??=[]).push(t,s),(e.preOrderCheckHooks??=[]).push(t,s)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function ox(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=o;s&&(t.contentHooks??=[]).push(-e,s),a&&((t.contentHooks??=[]).push(e,a),(t.contentCheckHooks??=[]).push(e,a)),c&&(t.viewHooks??=[]).push(-e,c),l&&((t.viewHooks??=[]).push(e,l),(t.viewCheckHooks??=[]).push(e,l)),u!=null&&(t.destroyHooks??=[]).push(e,u)}}function zu(t,n,e){sx(t,n,3,e)}function Uu(t,n,e,i){(t[ae]&3)===e&&sx(t,n,e,i)}function Pg(t,n){let e=t[ae];(e&3)===n&&(e&=16383,e+=1,t[ae]=e)}function sx(t,n,e,i){let r=i!==void 0?t[Qo]&65535:0,o=i??-1,s=n.length-1,a=0;for(let c=r;c<s;c++)if(typeof n[c+1]=="number"){if(a=n[c],i!=null&&a>=i)break}else n[c]<0&&(t[Qo]+=65536),(a<o||o==-1)&&(HT(t,e,n,c),t[Qo]=(t[Qo]&4294901760)+c+2),c++}function wC(t,n){Le(Ae.LifecycleHookStart,t,n);let e=re(null);try{n.call(t)}finally{re(e),Le(Ae.LifecycleHookEnd,t,n)}}function HT(t,n,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=t[s];r?t[ae]>>14<t[Qo]>>16&&(t[ae]&3)===n&&(t[ae]+=16384,wC(a,o)):wC(a,o)}var na=-1,ns=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function zT(t){return(t.flags&8)!==0}function UT(t){return(t.flags&16)!==0}function $T(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];t.setAttribute(n,s,a,o)}else{let o=r,s=e[++i];GT(o)?t.setProperty(n,o,s):t.setAttribute(n,o,s),i++}}return i}function ax(t){return t===3||t===4||t===6}function GT(t){return t.charCodeAt(0)===64}function ia(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?DC(t,e,r,null,n[++i]):DC(t,e,r,null,null))}}return t}function DC(t,n,e,i,r){let o=0,s=t.length;if(n===-1)s=-1;else for(;o<t.length;){let a=t[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<t.length;){let a=t[o];if(typeof a=="number")break;if(a===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(t.splice(s,0,n),o=s+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function cx(t){return t!==na}function Yu(t){return t&32767}function WT(t){return t>>16}function Zu(t,n){let e=WT(t),i=n;for(;e>0;)i=i[Zo],e--;return i}var Wg=!0;function Qu(t){let n=Wg;return Wg=t,n}var qT=256,lx=qT-1,dx=5,YT=0,Gi={};function ZT(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(Wo)&&(i=e[Wo]),i==null&&(i=e[Wo]=YT++);let r=i&lx,o=1<<r;n.data[t+(r>>dx)]|=o}function Xu(t,n){let e=ux(t,n);if(e!==-1)return e;let i=n[oe];i.firstCreatePass&&(t.injectorIndex=n.length,Fg(i.data,t),Fg(n,null),Fg(i.blueprint,null));let r=k_(t,n),o=t.injectorIndex;if(cx(r)){let s=Yu(r),a=Zu(r,n),c=a[oe].data;for(let l=0;l<8;l++)n[o+l]=a[s+l]|c[s+l]}return n[o+8]=r,o}function Fg(t,n){t.push(0,0,0,0,0,0,0,0,n)}function ux(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function k_(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=gx(r),i===null)return na;if(e++,r=r[Zo],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return na}function qg(t,n,e){ZT(t,n,e)}function QT(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(ax(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function mx(t,n,e){if(e&8||t!==void 0)return t;yu(n,"NodeInjector")}function fx(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[mr],o=hn(void 0);try{return r?r.get(n,i,e&8):eg(n,i,e&8)}finally{hn(o)}}return mx(i,n,e)}function hx(t,n,e,i=0,r){if(t!==null){if(n[ae]&2048&&!(i&2)){let s=eA(t,n,e,i,Gi);if(s!==Gi)return s}let o=px(t,n,e,i,Gi);if(o!==Gi)return o}return fx(n,e,i,r)}function px(t,n,e,i,r){let o=KT(e);if(typeof o=="function"){if(!Ig(n,t,i))return i&1?mx(r,e,i):fx(n,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))yu(e);else return s}finally{kg()}}else if(typeof o=="number"){let s=null,a=ux(t,n),c=na,l=i&1?n[gn][pn]:null;for((a===-1||i&4)&&(c=a===-1?k_(t,n):n[a+8],c===na||!SC(i,!1)?a=-1:(s=n[oe],a=Yu(c),n=Zu(c,n)));a!==-1;){let u=n[oe];if(EC(o,a,u.data)){let m=XT(a,n,e,s,i,l);if(m!==Gi)return m}c=n[a+8],c!==na&&SC(i,n[oe].data[a+8]===l)&&EC(o,a,n)?(s=u,a=Yu(c),n=Zu(c,n)):a=-1}}return r}function XT(t,n,e,i,r,o){let s=n[oe],a=s.data[t+8],c=i==null?Vi(a)&&Wg:i!=s&&(a.type&3)!==0,l=r&1&&o===a,u=$u(a,s,e,c,l);return u!==null?Vc(n,s,u,a,r):Gi}function $u(t,n,e,i,r){let o=t.providerIndexes,s=n.data,a=o&1048575,c=t.directiveStart,l=t.directiveEnd,u=o>>20,m=i?a:a+u,g=r?a+u:l;for(let _=m;_<g;_++){let x=s[_];if(_<c&&e===x||_>=c&&x.type===e)return _}if(r){let _=s[c];if(_&&Hi(_)&&_.type===e)return c}return null}function Vc(t,n,e,i,r){let o=t[e],s=n.data;if(o instanceof ns){let a=o;if(a.resolving)throw Jp("");let c=Qu(a.canSeeViewProviders);a.resolving=!0;let l=s[e].type||s[e],u,m=a.injectImpl?hn(a.injectImpl):null,g=Ig(t,i,0);try{o=t[e]=a.factory(void 0,r,s,t,i),n.firstCreatePass&&e>=i.directiveStart&&VT(e,s[e],n)}finally{m!==null&&hn(m),Qu(c),a.resolving=!1,kg()}}return o}function KT(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(Wo)?t[Wo]:void 0;return typeof n=="number"?n>=0?n&lx:JT:n}function EC(t,n,e){let i=1<<t;return!!(e[n+(t>>dx)]&i)}function SC(t,n){return!(t&2)&&!(t&1&&n)}var ts=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return hx(this._tNode,this._lView,n,Uo(i),e)}};function JT(){return new ts(Vt(),ne())}function lt(t){return Wc(()=>{let n=t.prototype.constructor,e=n[Cc]||Yg(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[Cc]||Yg(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function Yg(t){return $p(t)?()=>{let n=Yg(Qt(t));return n&&n()}:Vr(t)}function eA(t,n,e,i,r){let o=t,s=n;for(;o!==null&&s!==null&&s[ae]&2048&&!Qs(s);){let a=px(o,s,e,i|2,Gi);if(a!==Gi)return a;let c=o.parent;if(!c){let l=s[cg];if(l){let u=l.get(e,Gi,i&-5);if(u!==Gi)return u}c=gx(s),s=s[Zo]}o=c}return r}function gx(t){let n=t[oe],e=n.type;return e===2?n.declTNode:e===1?t[pn]:null}function qc(t){return QT(Vt(),t)}function tA(){return ca(Vt(),ne())}function ca(t,n){return new P(qn(t,n))}var P=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=tA}return t})();function _x(t){return t instanceof P?t.nativeElement:t}function nA(){return this._results[Symbol.iterator]()}var Dn=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new I}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=L0(n);(this._changesDetected=!F0(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=nA};function vx(t){return(t.flags&128)===128}var M_=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(M_||{}),bx=new Map,iA=0;function rA(){return iA++}function oA(t){bx.set(t[fr],t)}function Zg(t){bx.delete(t[fr])}var IC="__ngContext__";function ra(t,n){hr(n)?(t[IC]=n[fr],oA(n)):t[IC]=n}function yx(t){return xx(t[Ys])}function Cx(t){return xx(t[Gn])}function xx(t){for(;t!==null&&!fi(t);)t=t[Gn];return t}var sA;function T_(t){sA=t}var Qr=new y("",{factory:()=>aA}),aA="ng";var dm=new y(""),ss=new y("",{providedIn:"platform",factory:()=>"unknown"}),Yc=new y(""),as=new y("",{factory:()=>d(K).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var wx="r";var Dx="di";var Ex=!1,Sx=new y("",{factory:()=>Ex});var cA=(t,n,e,i)=>{};function lA(t,n,e,i){cA(t,n,e,i)}function um(t){return(t.flags&32)===32}var dA=()=>null;function Ix(t,n,e=!1){return dA(t,n,e)}function kx(t,n){let e=t.contentQueries;if(e!==null){let i=re(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=t.data[s];Rc(o),a.contentQueries(2,n[s],s)}}}finally{re(i)}}}function Qg(t,n,e){Rc(0);let i=re(null);try{n(t,e)}finally{re(i)}}function A_(t,n,e){if(dg(n)){let i=re(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=t.data[s];if(a.contentQueries){let c=e[s];a.contentQueries(1,c,s)}}}finally{re(i)}}}var _i=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(_i||{});var Bu;function uA(){if(Bu===void 0&&(Bu=null,$n.trustedTypes))try{Bu=$n.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Bu}function mm(t){return uA()?.createHTML(t)||t}var ju;function mA(){if(ju===void 0&&(ju=null,$n.trustedTypes))try{ju=$n.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return ju}function kC(t){return mA()?.createScriptURL(t)||t}var gr=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${gu})`}},Xg=class extends gr{getTypeName(){return"HTML"}},Kg=class extends gr{getTypeName(){return"Style"}},Jg=class extends gr{getTypeName(){return"Script"}},e_=class extends gr{getTypeName(){return"URL"}},t_=class extends gr{getTypeName(){return"ResourceURL"}};function vi(t){return t instanceof gr?t.changingThisBreaksApplicationSecurity:t}function _r(t,n){let e=Mx(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${gu})`)}return e===n}function Mx(t){return t instanceof gr&&t.getTypeName()||null}function R_(t){return new Xg(t)}function O_(t){return new Kg(t)}function N_(t){return new Jg(t)}function P_(t){return new e_(t)}function F_(t){return new t_(t)}function fA(t){let n=new i_(t);return hA()?new n_(n):n}var n_=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(mm(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},i_=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=mm(n),e}};function hA(){try{return!!new window.DOMParser().parseFromString(mm(""),"text/html")}catch{return!1}}var pA=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Zc(t){return t=String(t),t.match(pA)?t:"unsafe:"+t}function vr(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function Qc(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var Tx=vr("area,br,col,hr,img,wbr"),Ax=vr("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),Rx=vr("rp,rt"),gA=Qc(Rx,Ax),_A=Qc(Ax,vr("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),vA=Qc(Rx,vr("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),MC=Qc(Tx,_A,vA,gA),Ox=vr("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),bA=vr("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),yA=vr("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),CA=Qc(Ox,bA,yA),xA=vr("script,style,template");var r_=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=EA(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=DA(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=TC(n).toLowerCase();if(!MC.hasOwnProperty(e))return this.sanitizedSomething=!0,!xA.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!CA.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;Ox[a]&&(c=Zc(c)),this.buf.push(" ",s,'="',AC(c),'"')}return this.buf.push(">"),!0}endElement(n){let e=TC(n).toLowerCase();MC.hasOwnProperty(e)&&!Tx.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(AC(n))}};function wA(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function DA(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw Nx(n);return n}function EA(t){let n=t.firstChild;if(n&&wA(t,n))throw Nx(n);return n}function TC(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function Nx(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var SA=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,IA=/([^\#-~ |!])/g;function AC(t){return t.replace(/&/g,"&amp;").replace(SA,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(IA,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Vu;function L_(t,n){let e=null;try{Vu=Vu||fA(t);let i=n?String(n):"";e=Vu.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=Vu.getInertBodyElement(i)}while(i!==o);let a=new r_().sanitizeChildren(RC(e)||e);return mm(a)}finally{if(e){let i=RC(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function RC(t){return"content"in t&&kA(t)?t.content:null}function kA(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var MA=/^>|^->|<!--|-->|--!>|<!-$/g,TA=/(<|>)/g,AA="\u200B$1\u200B";function RA(t){return t.replace(MA,n=>n.replace(TA,AA))}function OA(t,n){return t.createText(n)}function NA(t,n,e){t.setValue(n,e)}function PA(t,n){return t.createComment(RA(n))}function Px(t,n,e){return t.createElement(n,e)}function Ku(t,n,e,i,r){t.insertBefore(n,e,i,r)}function Fx(t,n,e){t.appendChild(n,e)}function OC(t,n,e,i,r){i!==null?Ku(t,n,e,i,r):Fx(t,n,e)}function Lx(t,n,e,i){t.removeChild(null,n,e,i)}function FA(t,n,e){t.setAttribute(n,"style",e)}function LA(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function Bx(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&$T(t,n,i),r!==null&&LA(t,n,r),o!==null&&FA(t,n,o)}var Ht=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t})(Ht||{});function Zn(t){let n=Vx();return n?n.sanitize(Ht.URL,t)||"":_r(t,"URL")?vi(t):Zc(qo(t))}function jx(t){let n=Vx();if(n)return kC(n.sanitize(Ht.RESOURCE_URL,t)||"");if(_r(t,"ResourceURL"))return kC(vi(t));throw new A(904,!1)}var BA={embed:{src:!0},frame:{src:!0},iframe:{src:!0},media:{src:!0},script:{src:!0,href:!0,"xlink:href":!0},base:{href:!0},link:{href:!0},object:{data:!0,codebase:!0}};function jA(t,n){return BA[t]?.[n]===!0?jx:Zn}function B_(t,n,e){return jA(n,e)(t)}function Vx(){let t=ne();return t&&t[mi].sanitizer}function j_(t){return t.ownerDocument}function Hx(t){return t instanceof Function?t():t}function VA(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var zx="ng-template";function HA(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&VA(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(V_(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function V_(t){return t.type===4&&t.value!==zx}function zA(t,n,e){let i=t.type===4&&!e?zx:t.value;return n===i}function UA(t,n,e){let i=4,r=t.attrs,o=r!==null?WA(r):0,s=!1;for(let a=0;a<n.length;a++){let c=n[a];if(typeof c=="number"){if(!s&&!pi(i)&&!pi(c))return!1;if(s&&pi(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!zA(t,c,e)||c===""&&n.length===1){if(pi(i))return!1;s=!0}}else if(i&8){if(r===null||!HA(t,r,c,e)){if(pi(i))return!1;s=!0}}else{let l=n[++a],u=$A(c,r,V_(t),e);if(u===-1){if(pi(i))return!1;s=!0;continue}if(l!==""){let m;if(u>o?m="":m=r[u+1].toLowerCase(),i&2&&l!==m){if(pi(i))return!1;s=!0}}}}return pi(i)||s}function pi(t){return(t&1)===0}function $A(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let s=n[r];if(s===t)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return qA(n,t)}function Ux(t,n,e=!1){for(let i=0;i<n.length;i++)if(UA(t,n[i],e))return!0;return!1}function GA(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function WA(t){for(let n=0;n<t.length;n++){let e=t[n];if(ax(e))return n}return t.length}function qA(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function YA(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function NC(t,n){return t?":not("+n.trim()+")":n}function ZA(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let s=t[e];if(typeof s=="string")if(i&2){let a=t[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!pi(s)&&(n+=NC(o,r),r=""),i=s,o=o||!pi(i);e++}return r!==""&&(n+=NC(o,r)),n}function QA(t){return t.map(ZA).join(",")}function XA(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!pi(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var an={};function H_(t,n,e,i,r,o,s,a,c,l,u){let m=ot+i,g=m+r,_=KA(m,g),x=typeof l=="function"?l():l;return _[oe]={type:t,blueprint:_,template:e,queries:null,viewQuery:a,declTNode:n,data:_.slice().fill(null,m),bindingStartIndex:m,expandoStartIndex:g,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:x,incompleteFirstPass:!1,ssrId:u}}function KA(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:an);return e}function JA(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=H_(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function z_(t,n,e,i,r,o,s,a,c,l,u){let m=n.blueprint.slice();return m[ui]=r,m[ae]=i|4|128|8|64|1024,(l!==null||t&&t[ae]&2048)&&(m[ae]|=2048),hg(m),m[jt]=m[Zo]=t,m[St]=e,m[mi]=s||t&&t[mi],m[Ue]=a||t&&t[Ue],m[mr]=c||t&&t[mr]||null,m[pn]=o,m[fr]=rA(),m[Yo]=u,m[cg]=l,m[gn]=n.type==2?t[gn]:m,m}function eR(t,n,e){let i=qn(n,t),r=JA(e),o=t[mi].rendererFactory,s=U_(t,z_(t,r,null,$x(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=s}function $x(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function Gx(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function U_(t,n){return t[Ys]?t[ag][Gn]=n:t[Ys]=n,t[ag]=n,n}function p(t=1){Wx(Ze(),ne(),Ui()+t,!1)}function Wx(t,n,e,i){if(!i)if((n[ae]&3)===3){let o=t.preOrderCheckHooks;o!==null&&zu(n,o,e)}else{let o=t.preOrderHooks;o!==null&&Uu(n,o,0,e)}Yr(e)}var fm=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(fm||{});function o_(t,n,e,i){let r=re(null);try{let[o,s,a]=t.inputs[e],c=null;(s&fm.SignalBased)!==0&&(c=n[o][Dt]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(n,i)),t.setInput!==null?t.setInput(n,c,i,e,o):tx(n,c,o,i)}finally{re(r)}}var Wi=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Wi||{}),tR;function $_(t,n){return tR(t,n)}var r6=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var s_=new WeakMap,Lc=new WeakSet;function nR(t,n){let e=s_.get(t);if(!e||e.length===0)return;let i=n.parentNode,r=n.previousSibling;for(let o=e.length-1;o>=0;o--){let s=e[o],a=s.parentNode;s===n?(e.splice(o,1),Lc.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&s===r||a&&i&&a!==i)&&(e.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function iR(t,n){let e=s_.get(t);e?e.includes(n)||e.push(n):s_.set(t,[n])}var is=new Set,hm=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(hm||{}),bi=new y(""),PC=new Set;function Xr(t){PC.has(t)||(PC.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var pm=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=C({token:t,providedIn:"root",factory:()=>new t})}return t})(),G_=[0,1,2,3],W_=(()=>{class t{ngZone=d(j);scheduler=d(Fi);errorHandler=d(on,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(bi,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Le(Ae.AfterRenderHooksStart),this.executing=!0;for(let i of G_)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Le(Ae.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Xo]??=[]).push(e),es(i),i[ae]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(hm.AFTER_NEXT_RENDER,e):e()}static \u0275prov=C({token:t,providedIn:"root",factory:()=>new t})}return t})(),Hc=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,s=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Xo];n&&(this.view[Xo]=n.filter(e=>e!==this))}};function bt(t,n){let e=n?.injector??d(X);return Xr("NgAfterNextRender"),oR(t,e,n,!0)}function rR(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function oR(t,n,e,i){let r=n.get(pm);r.impl??=n.get(W_);let o=n.get(bi,null,{optional:!0}),s=e?.manualCleanup!==!0?n.get(sn):null,a=n.get(ea,null,{optional:!0}),c=new Hc(r.impl,rR(t),a?.view,i,s,o?.snapshot(null));return r.impl.register(c),c}var qx=new y("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:d(ze)})});function Yx(t,n,e){let i=t.get(qx);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function sR(t,n){let e=t.get(qx);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function aR(t,n){for(let[e,i]of n)Yx(t,i.animateFns)}function FC(t,n,e,i){let r=t?.[Wr]?.enter;n!==null&&r&&r.has(e.index)&&aR(i,r)}function ta(t,n,e,i,r,o,s,a){if(r!=null){let c,l=!1;fi(r)?c=r:hr(r)&&(l=!0,r=r[ui]);let u=Wn(r);t===0&&i!==null?(FC(a,i,o,e),s==null?Fx(n,i,u):Ku(n,i,u,s||null,!0)):t===1&&i!==null?(FC(a,i,o,e),Ku(n,i,u,s||null,!0),nR(o,u)):t===2?(a?.[Wr]?.leave?.has(o.index)&&iR(o,u),Lc.delete(u),LC(a,o,e,m=>{if(Lc.has(u)){Lc.delete(u);return}Lx(n,u,l,m)})):t===3&&(Lc.delete(u),LC(a,o,e,()=>{n.destroyNode(u)})),c!=null&&vR(n,t,e,c,o,i,s)}}function cR(t,n){Zx(t,n),n[ui]=null,n[pn]=null}function lR(t,n,e,i,r,o){i[ui]=r,i[pn]=n,_m(t,i,e,1,r,o)}function Zx(t,n){n[mi].changeDetectionScheduler?.notify(9),_m(t,n,n[Ue],2,null,null)}function dR(t){let n=t[Ys];if(!n)return Lg(t[oe],t);for(;n;){let e=null;if(hr(n))e=n[Ys];else{let i=n[vt];i&&(e=i)}if(!e){for(;n&&!n[Gn]&&n!==t;)hr(n)&&Lg(n[oe],n),n=n[jt];n===null&&(n=t),hr(n)&&Lg(n[oe],n),e=n&&n[Gn]}n=e}}function q_(t,n){let e=t[Ko],i=e.indexOf(n);e.splice(i,1)}function gm(t,n){if(Jo(n))return;let e=n[Ue];e.destroyNode&&_m(t,n,e,3,null,null),dR(n)}function Lg(t,n){if(Jo(n))return;let e=re(null);try{n[ae]&=-129,n[ae]|=256,n[On]&&Br(n[On]),fR(t,n),mR(t,n),n[oe].type===1&&n[Ue].destroy();let i=n[Gr];if(i!==null&&fi(n[jt])){i!==n[jt]&&q_(i,n);let r=n[ji];r!==null&&r.detachView(t)}Zg(n)}finally{re(e)}}function LC(t,n,e,i){let r=t?.[Wr];if(r==null||r.leave==null||!r.leave.has(n.index))return i(!1);t&&is.add(t[fr]),Yx(e,()=>{if(r.leave&&r.leave.has(n.index)){let s=r.leave.get(n.index),a=[];if(s){for(let c=0;c<s.animateFns.length;c++){let l=s.animateFns[c],{promise:u}=l();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),uR(t,i)}else t&&is.delete(t[fr]),i(!1)},r)}function uR(t,n){let e=t[Wr]?.running;if(e){e.then(()=>{t[Wr].running=void 0,is.delete(t[fr]),n(!0)});return}n(!1)}function mR(t,n){let e=t.cleanup,i=n[qs];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(n[qs]=null);let r=n[dr];if(r!==null){n[dr]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[Hr];if(o!==null){n[Hr]=null;for(let s of o)s.destroy()}}function fR(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof ns)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];Le(Ae.LifecycleHookStart,a,c);try{c.call(a)}finally{Le(Ae.LifecycleHookEnd,a,c)}}else{Le(Ae.LifecycleHookStart,r,o);try{o.call(r)}finally{Le(Ae.LifecycleHookEnd,r,o)}}}}}function Qx(t,n,e){return hR(t,n.parent,e)}function hR(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[ui];if(Vi(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===_i.None||r===_i.Emulated)return null}return qn(i,e)}function Xx(t,n,e){return gR(t,n,e)}function pR(t,n,e){return t.type&40?qn(t,e):null}var gR=pR,BC;function Y_(t,n,e,i){let r=Qx(t,i,n),o=n[Ue],s=i.parent||n[pn],a=Xx(s,i,n);if(r!=null)if(Array.isArray(e))for(let c=0;c<e.length;c++)OC(o,r,e[c],a,!1);else OC(o,r,e,a,!1);BC!==void 0&&BC(o,i,n,e,r)}function Bc(t,n){if(n!==null){let e=n.type;if(e&3)return qn(n,t);if(e&4)return a_(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return Bc(t,i);{let r=t[n.index];return fi(r)?a_(-1,r):Wn(r)}}else{if(e&128)return Bc(t,n.next);if(e&32)return $_(n,t)()||Wn(t[n.index]);{let i=Kx(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=zr(t[gn]);return Bc(r,i)}else return Bc(t,n.next)}}}return null}function Kx(t,n){if(n!==null){let i=t[gn][pn],r=n.projection;return i.projection[r]}return null}function a_(t,n){let e=vt+t+1;if(e<n.length){let i=n[e],r=i[oe].firstChild;if(r!==null)return Bc(i,r)}return n[qr]}function Z_(t,n,e,i,r,o,s){for(;e!=null;){let a=i[mr];if(e.type===128){e=e.next;continue}let c=i[e.index],l=e.type;if(s&&n===0&&(c&&ra(Wn(c),i),e.flags|=2),!um(e))if(l&8)Z_(t,n,e.child,i,r,o,!1),ta(n,t,a,r,c,e,o,i);else if(l&32){let u=$_(e,i),m;for(;m=u();)ta(n,t,a,r,m,e,o,i);ta(n,t,a,r,c,e,o,i)}else l&16?Jx(t,n,i,e,r,o):ta(n,t,a,r,c,e,o,i);e=s?e.projectionNext:e.next}}function _m(t,n,e,i,r,o){Z_(e,i,t.firstChild,n,r,o,!1)}function _R(t,n,e){let i=n[Ue],r=Qx(t,e,n),o=e.parent||n[pn],s=Xx(o,e,n);Jx(i,0,n,e,r,s)}function Jx(t,n,e,i,r,o){let s=e[gn],c=s[pn].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];ta(n,t,e[mr],r,u,i,o,e)}else{let l=c,u=s[jt];vx(i)&&(l.flags|=128),Z_(t,n,l,u,r,o,!0)}}function vR(t,n,e,i,r,o,s){let a=i[qr],c=Wn(i);a!==c&&ta(n,t,e,o,a,r,s);for(let l=vt;l<i.length;l++){let u=i[l];_m(u[oe],u,t,n,o,a)}}function bR(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Wi.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Wi.Important),t.setStyle(e,i,r,o))}}function ew(t,n,e,i,r){let o=Ui(),s=i&2;try{Yr(-1),s&&n.length>ot&&Wx(t,n,ot,!1);let a=s?Ae.TemplateUpdateStart:Ae.TemplateCreateStart;Le(a,r,e),e(i,r)}finally{Yr(o);let a=s?Ae.TemplateUpdateEnd:Ae.TemplateCreateEnd;Le(a,r,e)}}function vm(t,n,e){DR(t,n,e),(e.flags&64)===64&&ER(t,n,e)}function Xc(t,n,e=qn){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(n,t):t[s];t[r++]=a}}}function yR(t,n,e,i){let o=i.get(Sx,Ex)||e===_i.ShadowDom||e===_i.ExperimentalIsolatedShadowDom,s=t.selectRootElement(n,o);return CR(s),s}function CR(t){xR(t)}var xR=()=>null;function wR(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function tw(t,n,e,i,r,o){let s=n[oe];if(bm(t,s,n,e,i)){Vi(t)&&iw(n,t.index);return}t.type&3&&(e=wR(e)),nw(t,n,e,i,r,o)}function nw(t,n,e,i,r,o){if(t.type&3){let s=qn(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(s,e,i)}else t.type&12}function iw(t,n){let e=Yn(n,t);e[ae]&16||(e[ae]|=64)}function DR(t,n,e){let i=e.directiveStart,r=e.directiveEnd;Vi(e)&&eR(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||Xu(e,n);let o=e.initialInputs;for(let s=i;s<r;s++){let a=t.data[s],c=Vc(n,t,s,e);if(ra(c,n),o!==null&&kR(n,s-i,c,a,e,o),Hi(a)){let l=Yn(e.index,n);l[St]=Vc(n,t,s,e)}}}function ER(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=aC();try{Yr(o);for(let a=i;a<r;a++){let c=t.data[a],l=n[a];Au(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&SR(c,l)}}finally{Yr(-1),Au(s)}}function SR(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function Q_(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];Ux(n,o.selectors,!1)&&(i??=[],Hi(o)?i.unshift(o):i.push(o))}return i}function IR(t,n,e,i,r,o){let s=qn(t,n);rw(n[Ue],s,o,t.value,e,i,r)}function rw(t,n,e,i,r,o,s){if(o==null)t.removeAttribute(n,r,e);else{let a=s==null?qo(o):s(o,i||"",r);t.setAttribute(n,r,a,e)}}function kR(t,n,e,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];o_(i,e,c,l)}}function X_(t,n,e,i,r){let o=ot+e,s=n[oe],a=r(s,n,t,i,e);n[o]=a,Xs(t,!0);let c=t.type===2;return c?(Bx(n[Ue],a,t),(J0()===0||Zs(t))&&ra(a,n),eC()):ra(a,n),Pu()&&(!c||!um(t))&&Y_(s,n,a,t),t}function K_(t){let n=t;return Dg()?Eg():(n=n.parent,Xs(n,!1)),n}function MR(t,n){let e=t[mr];if(!e)return;let i;try{i=e.get(Pn,null)}catch{i=null}i?.(n)}function bm(t,n,e,i,r){let o=t.inputs?.[i],s=t.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],u=s[c+1],m=n.data[l];o_(m,e[l],u,r),a=!0}if(o)for(let c of o){let l=e[c],u=n.data[c];o_(u,l,i,r),a=!0}return a}function TR(t,n){let e=Yn(n,t),i=e[oe];AR(i,e);let r=e[ui];r!==null&&e[Yo]===null&&(e[Yo]=Ix(r,e[mr])),Le(Ae.ComponentStart);try{J_(i,e,e[St])}finally{Le(Ae.ComponentEnd,e[St])}}function AR(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function J_(t,n,e){Ou(n);try{let i=t.viewQuery;i!==null&&Qg(1,i,e);let r=t.template;r!==null&&ew(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[ji]?.finishViewCreation(t),t.staticContentQueries&&kx(t,n),t.staticViewQueries&&Qg(2,t.viewQuery,e);let o=t.components;o!==null&&RR(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[ae]&=-5,Nu()}}function RR(t,n){for(let e=0;e<n.length;e++)TR(t,n[e])}function Kc(t,n,e,i){let r=re(null);try{let o=n.tView,a=t[ae]&4096?4096:16,c=z_(t,o,e,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=t[n.index];c[Gr]=l;let u=t[ji];return u!==null&&(c[ji]=u.createEmbeddedView(o)),J_(o,c,e),c}finally{re(r)}}function oa(t,n){return!n||n.firstChild===null||vx(t)}function zc(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(Wn(o)),fi(o)&&ow(o,i);let s=e.type;if(s&8)zc(t,n,e.child,i);else if(s&32){let a=$_(e,n),c;for(;c=a();)i.push(c)}else if(s&16){let a=Kx(n,e);if(Array.isArray(a))i.push(...a);else{let c=zr(n[gn]);zc(c[oe],c,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function ow(t,n){for(let e=vt;e<t.length;e++){let i=t[e],r=i[oe].firstChild;r!==null&&zc(i[oe],i,r,n)}t[qr]!==t[ui]&&n.push(t[qr])}function sw(t){if(t[Xo]!==null){for(let n of t[Xo])n.impl.addSequence(n);t[Xo].length=0}}var aw=[];function OR(t){return t[On]??NR(t)}function NR(t){let n=aw.pop()??Object.create(FR);return n.lView=t,n}function PR(t){t.lView[On]!==t&&(t.lView=null,aw.push(t))}var FR=J(b({},Pr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{es(t.lView)},consumerOnSignalRead(){this.lView[On]=this}});function LR(t){let n=t[On]??Object.create(BR);return n.lView=t,n}var BR=J(b({},Pr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=zr(t.lView);for(;n&&!cw(n[oe]);)n=zr(n);n&&pg(n)},consumerOnSignalRead(){this.lView[On]=this}});function cw(t){return t.type!==2}function lw(t){if(t[Hr]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Hr])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[ae]&8192)}}var jR=100;function dw(t,n=0){let i=t[mi].rendererFactory,r=!1;r||i.begin?.();try{VR(t,n)}finally{r||i.end?.()}}function VR(t,n){let e=Sg();try{wc(!0),c_(t,n);let i=0;for(;Ac(t);){if(i===jR)throw new A(103,!1);i++,c_(t,1)}}finally{wc(e)}}function HR(t,n,e,i){if(Jo(n))return;let r=n[ae],o=!1,s=!1;Ou(n);let a=!0,c=null,l=null;o||(cw(t)?(l=OR(n),c=cr(l)):Td()===null?(a=!1,l=LR(n),c=cr(l)):n[On]&&(Br(n[On]),n[On]=null));try{hg(n),rC(t.bindingStartIndex),e!==null&&ew(t,n,e,2,i);let u=(r&3)===3;if(!o)if(u){let _=t.preOrderCheckHooks;_!==null&&zu(n,_,null)}else{let _=t.preOrderHooks;_!==null&&Uu(n,_,0,null),Pg(n,0)}if(s||zR(n),lw(n),uw(n,0),t.contentQueries!==null&&kx(t,n),!o)if(u){let _=t.contentCheckHooks;_!==null&&zu(n,_)}else{let _=t.contentHooks;_!==null&&Uu(n,_,1),Pg(n,1)}$R(t,n);let m=t.components;m!==null&&fw(n,m,0);let g=t.viewQuery;if(g!==null&&Qg(2,g,i),!o)if(u){let _=t.viewCheckHooks;_!==null&&zu(n,_)}else{let _=t.viewHooks;_!==null&&Uu(n,_,2),Pg(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[Du]){for(let _ of n[Du])_();n[Du]=null}o||(sw(n),n[ae]&=-73)}catch(u){throw o||es(n),u}finally{l!==null&&(Lr(l,c),a&&PR(l)),Nu()}}function uw(t,n){for(let e=yx(t);e!==null;e=Cx(e))for(let i=vt;i<e.length;i++){let r=e[i];mw(r,n)}}function zR(t){for(let n=yx(t);n!==null;n=Cx(n)){if(!(n[ae]&2))continue;let e=n[Ko];for(let i=0;i<e.length;i++){let r=e[i];pg(r)}}}function UR(t,n,e){Le(Ae.ComponentStart);let i=Yn(n,t);try{mw(i,e)}finally{Le(Ae.ComponentEnd,i[St])}}function mw(t,n){Iu(t)&&c_(t,n)}function c_(t,n){let i=t[oe],r=t[ae],o=t[On],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&Fs(o)),s||=!1,o&&(o.dirty=!1),t[ae]&=-9217,s)HR(i,t,i.template,t[St]);else if(r&8192){let a=re(null);try{lw(t),uw(t,1);let c=i.components;c!==null&&fw(t,c,1),sw(t)}finally{re(a)}}}function fw(t,n,e){for(let i=0;i<n.length;i++)UR(t,n[i],e)}function $R(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Yr(~r);else{let o=r,s=e[++i],a=e[++i];sC(s,o);let c=n[o];Le(Ae.HostBindingsUpdateStart,c);try{a(2,c)}finally{Le(Ae.HostBindingsUpdateEnd,c)}}}}finally{Yr(-1)}}function ev(t,n){let e=Sg()?64:1088;for(t[mi].changeDetectionScheduler?.notify(n);t;){t[ae]|=e;let i=zr(t);if(Qs(t)&&!i)return t;t=i}return null}function hw(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function pw(t,n){let e=vt+n;if(e<t.length)return t[e]}function Jc(t,n,e,i=!0){let r=n[oe];if(GR(r,n,t,e),i){let s=a_(e,t),a=n[Ue],c=a.parentNode(t[qr]);c!==null&&lR(r,t[pn],a,n,c,s)}let o=n[Yo];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function gw(t,n){let e=Uc(t,n);return e!==void 0&&gm(e[oe],e),e}function Uc(t,n){if(t.length<=vt)return;let e=vt+n,i=t[e];if(i){let r=i[Gr];r!==null&&r!==t&&q_(r,i),n>0&&(t[e-1][Gn]=i[Gn]);let o=kc(t,vt+n);cR(i[oe],i);let s=o[ji];s!==null&&s.detachView(o[oe]),i[jt]=null,i[Gn]=null,i[ae]&=-129}return i}function GR(t,n,e,i){let r=vt+i,o=e.length;i>0&&(e[r-1][Gn]=n),i<o-vt?(n[Gn]=e[r],tg(e,vt+i,n)):(e.push(n),n[Gn]=null),n[jt]=e;let s=n[Gr];s!==null&&e!==s&&_w(s,n);let a=n[ji];a!==null&&a.insertView(t),ku(n),n[ae]|=128}function _w(t,n){let e=t[Ko],i=n[jt];if(hr(i))t[ae]|=2;else{let r=i[jt][gn];n[gn]!==r&&(t[ae]|=2)}e===null?t[Ko]=[n]:e.push(n)}var Zr=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[oe];return zc(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[St]}set context(n){this._lView[St]=n}get destroyed(){return Jo(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[jt];if(fi(n)){let e=n[Tc],i=e?e.indexOf(this):-1;i>-1&&(Uc(n,i),kc(e,i))}this._attachedToViewContainer=!1}gm(this._lView[oe],this._lView)}onDestroy(n){gg(this._lView,n)}markForCheck(){ev(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[ae]&=-129}reattach(){ku(this._lView),this._lView[ae]|=128}detectChanges(){this._lView[ae]|=1024,dw(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new A(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Qs(this._lView),e=this._lView[Gr];e!==null&&!n&&q_(e,this._lView),Zx(this._lView[oe],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new A(902,!1);this._appRef=n;let e=Qs(this._lView),i=this._lView[Gr];i!==null&&!e&&_w(i,this._lView),ku(this._lView)}};var mt=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=WR;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Kc(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Zr(o)}}return t})();function WR(){return ym(Vt(),ne())}function ym(t,n){return t.type&4?new mt(n,t,ca(t,n)):null}function la(t,n,e,i,r){let o=t.data[n];if(o===null)o=qR(t,n,e,i,r),oC()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=tC();o.injectorIndex=s===null?-1:s.injectorIndex}return Xs(o,!0),o}function qR(t,n,e,i,r){let o=wg(),s=Dg(),a=s?o:o&&o.parent,c=t.data[n]=ZR(t,a,e,n,i,r);return YR(t,c,o,s),c}function YR(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function ZR(t,n,e,i,r,o){let s=n?n.injectorIndex:-1,a=0;return yg()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function QR(t){let n=t[lg]??[],i=t[jt][Ue],r=[];for(let o of n)o.data[Dx]!==void 0?r.push(o):XR(o,i);t[lg]=r}function XR(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[wx];for(;e<r;){let o=i.nextSibling;Lx(n,i,!1),i=o,e++}}}var KR=()=>null,JR=()=>null;function Ju(t,n){return KR(t,n)}function vw(t,n,e){return JR(t,n,e)}var bw=class{},Cm=class{},l_=class{resolveComponentFactory(n){throw new A(917,!1)}},el=class{static NULL=new l_},Ot=class{},we=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>e1()}return t})();function e1(){let t=ne(),n=Vt(),e=Yn(n.index,t);return(hr(e)?e:t)[Ue]}var yw=(()=>{class t{static \u0275prov=C({token:t,providedIn:"root",factory:()=>null})}return t})();var Gu={},d_=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Gu,i);return r!==Gu||e===Gu?r:this.parentInjector.get(n,e,i)}};function em(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=_u(r,a);else if(o==2){let c=a,l=n[++s];i=_u(i,c+": "+l+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function ie(t,n=0){let e=ne();if(e===null)return te(t,n);let i=Vt();return hx(i,e,Qt(t),n)}function xm(){let t="invalid";throw new Error(t)}function Cw(t,n,e,i,r){let o=i===null?null:{"":-1},s=r(t,e);if(s!==null){let a=s,c=null,l=null;for(let u of s)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(s);break}i1(t,n,e,a,o,c,l)}o!==null&&i!==null&&t1(e,i,o)}function t1(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new A(-301,!1);i.push(n[r],o)}}function n1(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function i1(t,n,e,i,r,o,s){let a=i.length,c=null;for(let g=0;g<a;g++){let _=i[g];c===null&&Hi(_)&&(c=_,n1(t,e,g)),qg(Xu(e,n),t,_.type)}l1(e,t.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let g=0;g<a;g++){let _=i[g];_.providersResolver&&_.providersResolver(_)}let l=!1,u=!1,m=Gx(t,n,a,null);a>0&&(e.directiveToIndex=new Map);for(let g=0;g<a;g++){let _=i[g];if(e.mergedAttrs=ia(e.mergedAttrs,_.hostAttrs),o1(t,e,n,m,_),c1(m,_,r),s!==null&&s.has(_)){let[O,B]=s.get(_);e.directiveToIndex.set(_.type,[m,O+e.directiveStart,B+e.directiveStart])}else(o===null||!o.has(_))&&e.directiveToIndex.set(_.type,m);_.contentQueries!==null&&(e.flags|=4),(_.hostBindings!==null||_.hostAttrs!==null||_.hostVars!==0)&&(e.flags|=64);let x=_.type.prototype;!l&&(x.ngOnChanges||x.ngOnInit||x.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),l=!0),!u&&(x.ngOnChanges||x.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),u=!0),m++}r1(t,e,o)}function r1(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))jC(0,n,r,i),jC(1,n,r,i),HC(n,i,!1);else{let o=e.get(r);VC(0,n,o,i),VC(1,n,o,i),HC(n,i,!0)}}}function jC(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;t===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),xw(n,o)}}function VC(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;t===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),xw(n,s)}}function xw(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function HC(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||V_(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!e&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===n){s??=[],s.push(c,i[a+1]);break}}else if(e&&o.hasOwnProperty(c)){let l=o[c];for(let u=0;u<l.length;u+=2)if(l[u]===n){s??=[],s.push(l[u+1],i[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function o1(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=Vr(r.type,!0)),s=new ns(o,Hi(r),ie,null);t.blueprint[i]=s,e[i]=s,s1(t,n,i,Gx(t,e,r.hostVars,an),r)}function s1(t,n,e,i,r){let o=r.hostBindings;if(o){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~n.index;a1(s)!=a&&s.push(a),s.push(e,i,o)}}function a1(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function c1(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;Hi(n)&&(e[""]=t)}}function l1(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function tv(t,n,e,i,r,o,s,a){let c=n[oe],l=c.consts,u=Nn(l,s),m=la(c,t,e,i,u);return o&&Cw(c,n,m,Nn(l,a),r),m.mergedAttrs=ia(m.mergedAttrs,m.attrs),m.attrs!==null&&em(m,m.attrs,!1),m.mergedAttrs!==null&&em(m,m.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,m),m}function nv(t,n){ox(t,n),dg(n)&&t.queries.elementEnd(n)}function d1(t,n,e,i,r,o){let s=n.consts,a=Nn(s,r),c=la(n,t,e,i,a);if(c.mergedAttrs=ia(c.mergedAttrs,c.attrs),o!=null){let l=Nn(s,o);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&em(c,c.attrs,!1),c.mergedAttrs!==null&&em(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}function wm(t,n,e){return t[n]=e}function u1(t,n){return t[n]}function _n(t,n,e){if(e===an)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function iv(t,n,e,i){let r=_n(t,n,e);return _n(t,n+1,i)||r}function m1(t,n,e,i,r){let o=iv(t,n,e,i);return _n(t,n+2,r)||o}function Wu(t,n,e){return function i(r){let o=Vi(t)?Yn(t.index,n):n;ev(o,5);let s=n[St],a=zC(n,s,e,r),c=i.__ngNextListenerFn__;for(;c;)a=zC(n,s,c,r)&&a,c=c.__ngNextListenerFn__;return a}}function zC(t,n,e,i){let r=re(null);try{return Le(Ae.OutputStart,n,e),e(i)!==!1}catch(o){return MR(t,o),!1}finally{Le(Ae.OutputEnd,n,e),re(r)}}function ww(t,n,e,i,r,o,s,a){let c=Zs(t),l=!1,u=null;if(!i&&c&&(u=h1(n,e,o,t.index)),u!==null){let m=u.__ngLastListenerFn__||u;m.__ngNextListenerFn__=s,u.__ngLastListenerFn__=s,l=!0}else{let m=qn(t,e),g=i?i(m):m;lA(e,g,o,a);let _=r.listen(g,o,a);if(!f1(o)){let x=i?O=>i(Wn(O[t.index])):t.index;Dw(x,n,e,o,a,_,!1)}}return l}function f1(t){return t.startsWith("animation")||t.startsWith("transition")}function h1(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=n[qs],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function Dw(t,n,e,i,r,o,s){let a=n.firstCreatePass?vg(n):null,c=_g(e),l=c.length;c.push(r,o),a&&a.push(i,t,l,(l+1)*(s?-1:1))}function UC(t,n,e,i,r,o){let s=n[e],a=n[oe],l=a.data[e].outputs[i],m=s[l].subscribe(o);Dw(t.index,a,n,r,o,m,!0)}var u_=Symbol("BINDING");function Ew(t){return t.debugInfo?.className||t.type.name||null}var tm=class extends el{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let e=ur(n);return new rs(e,this.ngModule)}};function p1(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&fm.SignalBased)!==0};return r&&(o.transform=r),o})}function g1(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function _1(t,n,e){let i=n instanceof ze?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new d_(e,i):e}function v1(t){let n=t.get(Ot,null);if(n===null)throw new A(407,!1);let e=t.get(yw,null),i=t.get(Fi,null),r=t.get(bi,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function b1(t,n){let e=Sw(t);return Px(n,e,e==="svg"?ug:e==="math"?q0:null)}function Sw(t){return(t.selectors[0][0]||"div").toLowerCase()}var rs=class extends Cm{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=p1(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=g1(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){super(),this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=QA(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,s){Le(Ae.DynamicComponentStart);let a=re(null);try{let c=this.componentDef,l=_1(c,r||this.ngModule,n),u=v1(l),m=u.tracingService;return m&&m.componentCreate?m.componentCreate(Ew(c),()=>this.createComponentRef(u,l,e,i,o,s)):this.createComponentRef(u,l,e,i,o,s)}finally{re(a)}}createComponentRef(n,e,i,r,o,s){let a=this.componentDef,c=y1(r,a,s,o),l=n.rendererFactory.createRenderer(null,a),u=r?yR(l,r,a.encapsulation,e):b1(a,l),m=s?.some($C)||o?.some(x=>typeof x!="function"&&x.bindings.some($C)),g=z_(null,c,null,512|$x(a),null,null,n,l,e,null,Ix(u,e,!0));g[ot]=u,Ou(g);let _=null;try{let x=tv(ot,g,2,"#host",()=>c.directiveRegistry,!0,0);Bx(l,u,x),ra(u,g),vm(c,g,x),A_(c,x,g),nv(c,x),i!==void 0&&x1(x,this.ngContentSelectors,i),_=Yn(x.index,g),g[St]=_[St],J_(c,g,null)}catch(x){throw _!==null&&Zg(_),Zg(g),x}finally{Le(Ae.DynamicComponentEnd),Nu()}return new nm(this.componentType,g,!!m)}};function y1(t,n,e,i){let r=t?["ng-version","21.2.10"]:XA(n.selectors[0]),o=null,s=null,a=0;if(e)for(let u of e)a+=u[u_].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let m=i[u];if(typeof m!="function")for(let g of m.bindings){a+=g[u_].requiredVars;let _=u+1;g.create&&(g.targetIdx=_,(o??=[]).push(g)),g.update&&(g.targetIdx=_,(s??=[]).push(g))}}let c=[n];if(i)for(let u of i){let m=typeof u=="function"?u:u.type,g=Kp(m);c.push(g)}return H_(0,null,C1(o,s),1,a,c,null,null,null,[r],null)}function C1(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function $C(t){let n=t[u_].kind;return n==="input"||n==="twoWay"}var nm=class extends bw{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=Eu(e[oe],ot),this.location=ca(this._tNode,e),this.instance=Yn(this._tNode.index,e)[St],this.hostView=this.changeDetectorRef=new Zr(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=bm(i,r[oe],r,n,e);this.previousInputValues.set(n,e);let s=Yn(i.index,r);ev(s,1)}get injector(){return new ts(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function x1(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Nt=(()=>{class t{static __NG_ELEMENT_ID__=w1}return t})();function w1(){let t=Vt();return Iw(t,ne())}var m_=class t extends Nt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return ca(this._hostTNode,this._hostLView)}get injector(){return new ts(this._hostTNode,this._hostLView)}get parentInjector(){let n=k_(this._hostTNode,this._hostLView);if(cx(n)){let e=Zu(n,this._hostLView),i=Yu(n),r=e[oe].data[i+8];return new ts(r,e)}else return new ts(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=GC(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-vt}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=Ju(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,oa(this._hostTNode,s)),a}createComponent(n,e,i,r,o,s,a){let c=n&&!FT(n),l;if(c)l=e;else{let B=e||{};l=B.index,i=B.injector,r=B.projectableNodes,o=B.environmentInjector||B.ngModuleRef,s=B.directives,a=B.bindings}let u=c?n:new rs(ur(n)),m=i||this.parentInjector;if(!o&&u.ngModule==null){let q=(c?m:this.parentInjector).get(ze,null);q&&(o=q)}let g=ur(u.componentType??{}),_=Ju(this._lContainer,g?.id??null),x=_?.firstChild??null,O=u.create(m,r,x,o,s,a);return this.insertImpl(O.hostView,l,oa(this._hostTNode,_)),O}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(Z0(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let c=r[jt],l=new t(c,c[pn],c[jt]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(e),s=this._lContainer;return Jc(s,r,o,i),n.attachToViewContainerRef(),tg(Bg(s),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=GC(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=Uc(this._lContainer,e);i&&(kc(Bg(this._lContainer),e),gm(i[oe],i))}detach(n){let e=this._adjustIndex(n,-1),i=Uc(this._lContainer,e);return i&&kc(Bg(this._lContainer),e)!=null?new Zr(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function GC(t){return t[Tc]}function Bg(t){return t[Tc]||(t[Tc]=[])}function Iw(t,n){let e,i=n[t.index];return fi(i)?e=i:(e=hw(i,n,null,t),n[t.index]=e,U_(n,e)),E1(e,n,t,i),new m_(e,t,n)}function D1(t,n){let e=t[Ue],i=e.createComment(""),r=qn(n,t),o=e.parentNode(r);return Ku(e,o,i,e.nextSibling(r),!1),i}var E1=k1,S1=()=>!1;function I1(t,n,e){return S1(t,n,e)}function k1(t,n,e,i){if(t[qr])return;let r;e.type&8?r=Wn(i):r=D1(n,e),t[qr]=r}var f_=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},h_=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)ov(n,e).matches!==null&&this.queries[e].setDirty()}},im=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=O1(n):this.predicate=n}},p_=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},g_=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,M1(e,o)),this.matchTNodeWithReadOption(n,e,$u(e,n,o,!1,!1))}else i===mt?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,$u(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===P||r===Nt||r===mt&&e.type&4)this.addMatch(e.index,-2);else{let o=$u(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function M1(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function T1(t,n){return t.type&11?ca(t,n):t.type&4?ym(t,n):null}function A1(t,n,e,i){return e===-1?T1(n,t):e===-2?R1(t,n,i):Vc(t,t[oe],e,n)}function R1(t,n,e){if(e===P)return ca(n,t);if(e===mt)return ym(n,t);if(e===Nt)return Iw(n,t)}function kw(t,n,e,i){let r=n[ji].queries[i];if(r.matches===null){let o=t.data,s=e.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let u=o[l];a.push(A1(n,u,s[c+1],e.metadata.read))}}r.matches=a}return r.matches}function __(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let s=kw(t,n,r,e);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],u=n[-c];for(let m=vt;m<u.length;m++){let g=u[m];g[Gr]===g[jt]&&__(g[oe],g,l,i)}if(u[Ko]!==null){let m=u[Ko];for(let g=0;g<m.length;g++){let _=m[g];__(_[oe],_,l,i)}}}}}return i}function rv(t,n){return t[ji].queries[n].queryList}function Mw(t,n,e){let i=new Dn((e&4)===4);return K0(t,n,i,i.destroy),(n[ji]??=new h_).queries.push(new f_(i))-1}function Tw(t,n,e){let i=Ze();return i.firstCreatePass&&(Rw(i,new im(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),Mw(i,ne(),n)}function Aw(t,n,e,i){let r=Ze();if(r.firstCreatePass){let o=Vt();Rw(r,new im(n,e,i),o.index),N1(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return Mw(r,ne(),e)}function O1(t){return t.split(",").map(n=>n.trim())}function Rw(t,n,e){t.queries===null&&(t.queries=new p_),t.queries.track(new g_(n,e))}function N1(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function ov(t,n){return t.queries.getByIndex(n)}function Ow(t,n){let e=t[oe],i=ov(e,n);return i.crossesNgTemplate?__(e,t,n,[]):kw(e,t,i,n)}function Nw(t,n,e){let i,r=uc(()=>{i._dirtyCounter();let o=P1(i,t);if(n&&o===void 0)throw new A(-951,!1);return o});return i=r[Dt],i._dirtyCounter=N(0),i._flatValue=void 0,r}function sv(t){return Nw(!0,!1,t)}function av(t){return Nw(!0,!0,t)}function Pw(t,n){let e=t[Dt];e._lView=ne(),e._queryIndex=n,e._queryList=rv(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function P1(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[ae]&4)return n?void 0:rn;let r=rv(e,i),o=Ow(e,i);return r.reset(o,_x),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}var qi=class{},Dm=class{};var rm=class extends qi{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new tm(this);constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=Xp(n);this._bootstrapComponents=Hx(o.bootstrap),this._r3Injector=Mg(n,e,[{provide:qi,useValue:this},{provide:el,useValue:this.componentFactoryResolver},...i],Sc(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},om=class extends Dm{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new rm(this.moduleType,n,[])}};var $c=class extends qi{injector;componentFactoryResolver=new tm(this);instance=null;constructor(n){super();let e=new Go([...n.providers,{provide:qi,useValue:this},{provide:el,useValue:this.componentFactoryResolver}],n.parent||Ws(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function tl(t,n,e=null){return new $c({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var F1=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=rg(!1,e.type),r=i.length>0?tl([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=C({token:t,providedIn:"environment",factory:()=>new t(te(ze))})}return t})();function E(t){return Wc(()=>{let n=Fw(t),e=J(b({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection===M_.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(F1).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||_i.Emulated,styles:t.styles||rn,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&Xr("NgStandalone"),Lw(e);let i=t.dependencies;return e.directiveDefs=WC(i,L1),e.pipeDefs=WC(i,R0),e.id=V1(e),e})}function L1(t){return ur(t)||Kp(t)}function W(t){return Wc(()=>({type:t.type,bootstrap:t.bootstrap||rn,declarations:t.declarations||rn,imports:t.imports||rn,exports:t.exports||rn,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function B1(t,n){if(t==null)return Ur;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=fm.None,c=null),e[o]=[i,a,c],n[o]=s}return e}function j1(t){if(t==null)return Ur;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function T(t){return Wc(()=>{let n=Fw(t);return Lw(n),n})}function Kr(t){return{type:t.type,name:t.name,factory:null,pure:t.pure!==!1,standalone:t.standalone??!0,onDestroy:t.type.prototype.ngOnDestroy||null}}function Fw(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Ur,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||rn,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:B1(t.inputs,n),outputs:j1(t.outputs),debugInfo:null}}function Lw(t){t.features?.forEach(n=>n(t))}function WC(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function V1(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function H1(t){return Object.getPrototypeOf(t.prototype).constructor}function _e(t){let n=H1(t.type),e=!0,i=[t];for(;n;){let r;if(Hi(t))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new A(903,!1);r=n.\u0275dir}if(r){if(e){i.push(r);let s=t;s.inputs=jg(t.inputs),s.declaredInputs=jg(t.declaredInputs),s.outputs=jg(t.outputs);let a=r.hostBindings;a&&W1(t,a);let c=r.viewQuery,l=r.contentQueries;if(c&&$1(t,c),l&&G1(t,l),z1(t,r),A0(t.outputs,r.outputs),Hi(r)&&r.data.animation){let u=t.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(t),a===_e&&(e=!1)}}n=Object.getPrototypeOf(n)}U1(i)}function z1(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function U1(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=ia(r.hostAttrs,e=ia(e,r.hostAttrs))}}function jg(t){return t===Ur?{}:t===rn?[]:t}function $1(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function G1(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function W1(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function Bw(t,n,e,i,r,o,s,a){if(e.firstCreatePass){t.mergedAttrs=ia(t.mergedAttrs,t.attrs);let u=t.tView=H_(2,t,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),u.queries=e.queries.embeddedTView(t))}a&&(t.flags|=a),Xs(t,!1);let c=Y1(e,n,t,i);Pu()&&Y_(e,n,c,t),ra(c,n);let l=hw(c,n,c,t);n[i+ot]=l,U_(n,l),I1(l,t,n)}function q1(t,n,e,i,r,o,s,a,c,l,u){let m=e+ot,g;return n.firstCreatePass?(g=la(n,m,4,s||null,a||null),Mu()&&Cw(n,t,g,Nn(n.consts,l),Q_),ox(n,g)):g=n.data[m],Bw(g,t,n,e,i,r,o,c),Zs(g)&&vm(n,t,g),l!=null&&Xc(t,g,u),g}function sa(t,n,e,i,r,o,s,a,c,l,u){let m=e+ot,g;if(n.firstCreatePass){if(g=la(n,m,4,s||null,a||null),l!=null){let _=Nn(n.consts,l);g.localNames=[];for(let x=0;x<_.length;x+=2)g.localNames.push(_[x],-1)}}else g=n.data[m];return Bw(g,t,n,e,i,r,o,c),l!=null&&Xc(t,g,u),g}function Be(t,n,e,i,r,o,s,a){let c=ne(),l=Ze(),u=Nn(l.consts,o);return q1(c,l,t,n,e,i,r,u,void 0,s,a),Be}function da(t,n,e,i,r,o,s,a){let c=ne(),l=Ze(),u=Nn(l.consts,o);return sa(c,l,t,n,e,i,r,u,void 0,s,a),da}var Y1=Z1;function Z1(t,n,e,i){return Oc(!0),n[Ue].createComment("")}var Em=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();function Yi(t){return typeof t=="function"&&t[Dt]!==void 0}function cv(t){return Yi(t)&&typeof t.set=="function"}var lv=new y("");function Jr(t){return!!t&&typeof t.then=="function"}function dv(t){return!!t&&typeof t.subscribe=="function"}var uv=new y("");function Sm(t){return Bi([{provide:uv,multi:!0,useValue:t}])}var mv=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(uv,{optional:!0})??[];injector=d(X);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=Gt(this.injector,r);if(Jr(o))e.push(o);else if(dv(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Im=new y("");function jw(){mp(()=>{let t="";throw new A(600,t)})}function Vw(t){return t.isBoundToModule}var Q1=10;var En=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(Pn);afterRenderManager=d(pm);zonelessEnabled=d(Nc);rootEffectScheduler=d(Lu);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new I;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(pr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ee(e=>!e))}constructor(){d(bi,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(ze);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=X.NULL){return this._injector.get(j).run(()=>{Le(Ae.BootstrapComponentStart);let s=e instanceof Cm;if(!this._injector.get(mv).done){let x="";throw new A(405,x)}let c;s?c=e:c=this._injector.get(el).resolveComponentFactory(e),this.componentTypes.push(c.componentType);let l=Vw(c)?void 0:this._injector.get(qi),u=i||c.selector,m=c.create(r,[],u,l),g=m.location.nativeElement,_=m.injector.get(lv,null);return _?.registerApplication(g),m.onDestroy(()=>{this.detachView(m.hostView),jc(this.components,m),_?.unregisterApplication(g)}),this._loadComponent(m),Le(Ae.BootstrapComponentEnd,m),m})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Le(Ae.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(hm.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Le(Ae.ChangeDetectionEnd),new A(101,!1);let e=re(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,re(e),this.afterTick.next(),Le(Ae.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ot,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<Q1;){Le(Ae.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Le(Ae.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Ac(r))continue;let o=i&&!this.zonelessEnabled?0:1;dw(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>Ac(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;jc(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Im,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>jc(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new A(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function jc(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function km(t,n){let e=ne(),i=zi();if(_n(e,i,n)){let r=Ze(),o=Js();if(bm(o,r,e,t,n))Vi(o)&&iw(e,o.index);else{let a=qn(o,e);rw(e[Ue],a,null,o.value,t,n,null)}}return km}function Y(t,n,e,i){let r=ne(),o=zi();if(_n(r,o,n)){let s=Ze(),a=Js();IR(a,r,t,n,e,i)}return Y}var v_=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function Vg(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function X1(t,n,e,i){let r,o,s=0,a=t.length-1,c=void 0;if(Array.isArray(n)){re(i);let l=n.length-1;for(re(null);s<=a&&s<=l;){let u=t.at(s),m=n[s],g=Vg(s,u,s,m,e);if(g!==0){g<0&&t.updateValue(s,m),s++;continue}let _=t.at(a),x=n[l],O=Vg(a,_,l,x,e);if(O!==0){O<0&&t.updateValue(a,x),a--,l--;continue}let B=e(s,u),q=e(a,_),be=e(s,m);if(Object.is(be,q)){let wt=e(l,x);Object.is(wt,B)?(t.swap(s,a),t.updateValue(a,x),l--,a--):t.move(a,s),t.updateValue(s,m),s++;continue}if(r??=new sm,o??=YC(t,s,a,e),b_(t,r,s,be))t.updateValue(s,m),s++,a++;else if(o.has(be))r.set(B,t.detach(s)),a--;else{let wt=t.create(s,n[s]);t.attach(s,wt),s++,a++}}for(;s<=l;)qC(t,r,e,s,n[s]),s++}else if(n!=null){re(i);let l=n[Symbol.iterator]();re(null);let u=l.next();for(;!u.done&&s<=a;){let m=t.at(s),g=u.value,_=Vg(s,m,s,g,e);if(_!==0)_<0&&t.updateValue(s,g),s++,u=l.next();else{r??=new sm,o??=YC(t,s,a,e);let x=e(s,g);if(b_(t,r,s,x))t.updateValue(s,g),s++,a++,u=l.next();else if(!o.has(x))t.attach(s,t.create(s,g)),s++,a++,u=l.next();else{let O=e(s,m);r.set(O,t.detach(s)),a--}}}for(;!u.done;)qC(t,r,e,t.length,u.value),u=l.next()}for(;s<=a;)t.destroy(t.detach(a--));r?.forEach(l=>{t.destroy(l)})}function b_(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function qC(t,n,e,i,r){if(b_(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function YC(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var sm=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function F(t,n,e,i,r,o,s,a){Xr("NgControlFlow");let c=ne(),l=Ze(),u=Nn(l.consts,o);return sa(c,l,t,n,e,i,r,u,256,s,a),fv}function fv(t,n,e,i,r,o,s,a){Xr("NgControlFlow");let c=ne(),l=Ze(),u=Nn(l.consts,o);return sa(c,l,t,n,e,i,r,u,512,s,a),fv}function L(t,n){Xr("NgControlFlow");let e=ne(),i=zi(),r=e[i]!==an?e[i]:-1,o=r!==-1?am(e,ot+r):void 0,s=0;if(_n(e,i,t)){let a=re(null);try{if(o!==void 0&&gw(o,s),t!==-1){let c=ot+t,l=am(e,c),u=w_(e[oe],c),m=vw(l,u,e),g=Kc(e,u,n,{dehydratedView:m});Jc(l,g,s,oa(u,m))}}finally{re(a)}}else if(o!==void 0){let a=pw(o,s);a!==void 0&&(a[St]=n)}}var y_=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-vt}};function eo(t){return t}function cs(t,n){return n}var C_=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function st(t,n,e,i,r,o,s,a,c,l,u,m,g){Xr("NgControlFlow");let _=ne(),x=Ze(),O=c!==void 0,B=ne(),q=a?s.bind(B[gn][St]):s,be=new C_(O,q);B[ot+t]=be,sa(_,x,t+1,n,e,i,r,Nn(x.consts,o),256),O&&sa(_,x,t+2,c,l,u,m,Nn(x.consts,g),512)}var x_=class extends v_{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-vt}at(n){return this.getLView(n)[St].$implicit}attach(n,e){let i=e[Yo];this.needsIndexUpdate||=n!==this.length,Jc(this.lContainer,e,n,oa(this.templateTNode,i)),K1(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,J1(this.lContainer,n),eO(this.lContainer,n)}create(n,e){let i=Ju(this.lContainer,this.templateTNode.tView.ssrId);return Kc(this.hostLView,this.templateTNode,new y_(this.lContainer,e,n),{dehydratedView:i})}destroy(n){gm(n[oe],n)}updateValue(n,e){this.getLView(n)[St].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[St].$index=n}getLView(n){return tO(this.lContainer,n)}};function at(t){let n=re(null),e=Ui();try{let i=ne(),r=i[oe],o=i[e],s=e+1,a=am(i,s);if(o.liveCollection===void 0){let l=w_(r,s);o.liveCollection=new x_(a,i,l)}else o.liveCollection.reset();let c=o.liveCollection;if(X1(c,t,o.trackByFn,n),c.updateIndexes(),o.hasEmptyBlock){let l=zi(),u=c.length===0;if(_n(i,l,u)){let m=e+2,g=am(i,m);if(u){let _=w_(r,m),x=vw(g,_,i),O=Kc(i,_,void 0,{dehydratedView:x});Jc(g,O,0,oa(_,x))}else r.firstUpdatePass&&QR(g),gw(g,0)}}}finally{re(n)}}function am(t,n){return t[n]}function K1(t,n){if(t.length<=vt)return;let e=vt+n,i=t[e],r=i?i[Wr]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[mr];sR(o,r),is.delete(i[fr]),r.detachedLeaveAnimationFns=void 0}}function J1(t,n){if(t.length<=vt)return;let e=vt+n,i=t[e],r=i?i[Wr]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function eO(t,n){return Uc(t,n)}function tO(t,n){return pw(t,n)}function w_(t,n){return Eu(t,n)}function D(t,n,e){let i=ne(),r=zi();if(_n(i,r,n)){let o=Ze(),s=Js();tw(s,i,t,n,i[Ue],e)}return D}function D_(t,n,e,i,r){bm(n,t,e,r?"class":"style",i)}function f(t,n,e,i){let r=ne(),o=r[oe],s=t+ot,a=o.firstCreatePass?tv(s,r,2,n,Q_,Mu(),e,i):o.data[s];if(Vi(a)){let c=r[mi].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(Ew(l),()=>(ZC(t,n,r,a,i),f))}}return ZC(t,n,r,a,i),f}function ZC(t,n,e,i,r){if(X_(i,e,t,n,Hw),Zs(i)){let o=e[oe];vm(o,e,i),A_(o,i,e)}r!=null&&Xc(e,i)}function h(){let t=Ze(),n=Vt(),e=K_(n);return t.firstCreatePass&&nv(t,e),Cg(e)&&xg(),bg(),e.classesWithoutHost!=null&&zT(e)&&D_(t,e,ne(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&UT(e)&&D_(t,e,ne(),e.stylesWithoutHost,!1),h}function M(t,n,e,i){return f(t,n,e,i),h(),M}function Ne(t,n,e,i){let r=ne(),o=r[oe],s=t+ot,a=o.firstCreatePass?d1(s,o,2,n,e,i):o.data[s];return X_(a,r,t,n,Hw),i!=null&&Xc(r,a),Ne}function Ge(){let t=Vt(),n=K_(t);return Cg(n)&&xg(),bg(),Ge}function zt(t,n,e,i){return Ne(t,n,e,i),Ge(),zt}var Hw=(t,n,e,i,r)=>(Oc(!0),Px(n[Ue],i,fC()));function hv(t,n,e){let i=ne(),r=i[oe],o=t+ot,s=r.firstCreatePass?tv(o,i,8,"ng-container",Q_,Mu(),n,e):r.data[o];if(X_(s,i,t,"ng-container",nO),Zs(s)){let a=i[oe];vm(a,i,s),A_(a,s,i)}return e!=null&&Xc(i,s),hv}function pv(){let t=Ze(),n=Vt(),e=K_(n);return t.firstCreatePass&&nv(t,e),pv}function cn(t,n,e){return hv(t,n,e),pv(),cn}var nO=(t,n,e,i,r)=>(Oc(!0),PA(n[Ue],""));function dt(){return ne()}function ft(t,n,e){let i=ne(),r=zi();if(_n(i,r,n)){let o=Ze(),s=Js();nw(s,i,t,n,i[Ue],e)}return ft}var Fc=void 0;function iO(t){let n=Math.floor(Math.abs(t)),e=t.toString().replace(/^[^.]*\.?/,"").length;return n===1&&e===0?1:5}var rO=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],Fc,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],Fc,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",Fc,Fc,Fc],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",iO],Hg={};function vn(t){let n=oO(t),e=QC(n);if(e)return e;let i=n.split("-")[0];if(e=QC(i),e)return e;if(i==="en")return rO;throw new A(701,!1)}function QC(t){return t in Hg||(Hg[t]=$n.ng&&$n.ng.common&&$n.ng.common.locales&&$n.ng.common.locales[t]),Hg[t]}var ht=(function(t){return t[t.LocaleId=0]="LocaleId",t[t.DayPeriodsFormat=1]="DayPeriodsFormat",t[t.DayPeriodsStandalone=2]="DayPeriodsStandalone",t[t.DaysFormat=3]="DaysFormat",t[t.DaysStandalone=4]="DaysStandalone",t[t.MonthsFormat=5]="MonthsFormat",t[t.MonthsStandalone=6]="MonthsStandalone",t[t.Eras=7]="Eras",t[t.FirstDayOfWeek=8]="FirstDayOfWeek",t[t.WeekendRange=9]="WeekendRange",t[t.DateFormat=10]="DateFormat",t[t.TimeFormat=11]="TimeFormat",t[t.DateTimeFormat=12]="DateTimeFormat",t[t.NumberSymbols=13]="NumberSymbols",t[t.NumberFormats=14]="NumberFormats",t[t.CurrencyCode=15]="CurrencyCode",t[t.CurrencySymbol=16]="CurrencySymbol",t[t.CurrencyName=17]="CurrencyName",t[t.Currencies=18]="Currencies",t[t.Directionality=19]="Directionality",t[t.PluralCase=20]="PluralCase",t[t.ExtraData=21]="ExtraData",t})(ht||{});function oO(t){return t.toLowerCase().replace(/_/g,"-")}var nl="en-US",sO="USD";var aO=nl;function zw(t){typeof t=="string"&&(aO=t.toLowerCase().replace(/_/g,"-"))}function R(t,n,e){let i=ne(),r=Ze(),o=Vt();return Uw(r,i,i[Ue],o,t,n,e),R}function ua(t,n,e){let i=ne(),r=Ze(),o=Vt();return(o.type&3||e)&&ww(o,r,i,e,i[Ue],t,n,Wu(o,i,n)),ua}function Uw(t,n,e,i,r,o,s){let a=!0,c=null;if((i.type&3||s)&&(c??=Wu(i,n,o),ww(i,t,n,s,e,r,o,c)&&(a=!1)),a){let l=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let m=0;m<u.length;m+=2){let g=u[m],_=u[m+1];c??=Wu(i,n,o),UC(i,n,g,_,r,c)}if(l&&l.length)for(let m of l)c??=Wu(i,n,o),UC(i,n,m,r,r,c)}}function S(t=1){return mC(t)}function cO(t,n){let e=null,i=GA(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?Ux(t,o,!0):YA(i,o))return r}return e}function Se(t){let n=ne()[gn][pn];if(!n.projection){let e=t?t.length:1,i=n.projection=B0(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=t?cO(o,t):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function se(t,n=0,e,i,r,o){let s=ne(),a=Ze(),c=i?t+1:null;c!==null&&sa(s,a,c,i,r,o,null,e);let l=la(a,ot+t,16,null,e||null);l.projection===null&&(l.projection=n),Eg();let m=!s[Yo]||yg();s[gn][pn].projection[l.projection]===null&&c!==null?lO(s,a,c):m&&!um(l)&&_R(a,s,l)}function lO(t,n,e){let i=ot+e,r=n.data[i],o=t[i],s=Ju(o,r.tView.ssrId),a=Kc(t,r,void 0,{dehydratedView:s});Jc(o,a,0,oa(r,s))}function yt(t,n,e,i){return Aw(t,n,e,i),yt}function je(t,n,e){return Tw(t,n,e),je}function U(t){let n=ne(),e=Ze(),i=Ru();Rc(i+1);let r=ov(e,i);if(t.dirty&&Y0(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=Ow(n,i);t.reset(o,_x),t.notifyOnChanges()}return!0}return!1}function $(){return rv(ne(),Ru())}function Mm(t,n,e,i,r){return Pw(n,Aw(t,e,i,r)),Mm}function Tm(t,n,e,i){return Pw(t,Tw(n,e,i)),Tm}function Am(t=1){Rc(Ru()+t)}function Ve(t){let n=nC();return Su(n,ot+t)}function Hu(t,n){return t<<17|n<<2}function os(t){return t>>17&32767}function dO(t){return(t&2)==2}function uO(t,n){return t&131071|n<<17}function E_(t){return t|2}function aa(t){return(t&131068)>>2}function zg(t,n){return t&-131069|n<<2}function mO(t){return(t&1)===1}function S_(t){return t|1}function fO(t,n,e,i,r,o){let s=o?n.classBindings:n.styleBindings,a=os(s),c=aa(s);t[i]=e;let l=!1,u;if(Array.isArray(e)){let m=e;u=m[1],(u===null||Gs(m,u)>0)&&(l=!0)}else u=e;if(r)if(c!==0){let g=os(t[a+1]);t[i+1]=Hu(g,a),g!==0&&(t[g+1]=zg(t[g+1],i)),t[a+1]=uO(t[a+1],i)}else t[i+1]=Hu(a,0),a!==0&&(t[a+1]=zg(t[a+1],i)),a=i;else t[i+1]=Hu(c,0),a===0?a=i:t[c+1]=zg(t[c+1],i),c=i;l&&(t[i+1]=E_(t[i+1])),XC(t,u,i,!0),XC(t,u,i,!1),hO(n,u,t,i,o),s=Hu(a,c),o?n.classBindings=s:n.styleBindings=s}function hO(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&Gs(o,n)>=0&&(e[i+1]=S_(e[i+1]))}function XC(t,n,e,i){let r=t[e+1],o=n===null,s=i?os(r):aa(r),a=!1;for(;s!==0&&(a===!1||o);){let c=t[s],l=t[s+1];pO(c,n)&&(a=!0,t[s+1]=i?S_(l):E_(l)),s=i?os(l):aa(l)}a&&(t[e+1]=i?E_(r):S_(r))}function pO(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?Gs(t,n)>=0:!1}var gi={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function gO(t){return t.substring(gi.key,gi.keyEnd)}function _O(t){return vO(t),$w(t,Gw(t,0,gi.textEnd))}function $w(t,n){let e=gi.textEnd;return e===n?-1:(n=gi.keyEnd=bO(t,gi.key=n,e),Gw(t,n,e))}function vO(t){gi.key=0,gi.keyEnd=0,gi.value=0,gi.valueEnd=0,gi.textEnd=t.length}function Gw(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function bO(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function Qn(t,n,e){return Ww(t,n,e,!1),Qn}function z(t,n){return Ww(t,n,null,!0),z}function It(t){CO(IO,yO,t,!0)}function yO(t,n){for(let e=_O(n);e>=0;e=$w(n,e))xu(t,gO(n),!0)}function Ww(t,n,e,i){let r=ne(),o=Ze(),s=Tu(2);if(o.firstUpdatePass&&Yw(o,t,s,i),n!==an&&_n(r,s,n)){let a=o.data[Ui()];Zw(o,a,r,r[Ue],t,r[s+1]=MO(n,e),i,s)}}function CO(t,n,e,i){let r=Ze(),o=Tu(2);r.firstUpdatePass&&Yw(r,null,o,i);let s=ne();if(e!==an&&_n(s,o,e)){let a=r.data[Ui()];if(Qw(a,i)&&!qw(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(e=_u(c,e||"")),D_(r,a,s,e,i)}else kO(r,a,s,s[Ue],s[o+1],s[o+1]=SO(t,n,e),i,o)}}function qw(t,n){return n>=t.expandoStartIndex}function Yw(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[Ui()],s=qw(t,e);Qw(o,i)&&n===null&&!s&&(n=!1),n=xO(r,o,n,i),fO(r,o,n,e,s,i)}}function xO(t,n,e,i){let r=cC(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=Ug(null,t,n,e,i),e=Gc(e,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||t[s]!==r)if(e=Ug(r,t,n,e,i),o===null){let c=wO(t,n,i);c!==void 0&&Array.isArray(c)&&(c=Ug(null,t,n,c[1],i),c=Gc(c,n.attrs,i),DO(t,n,i,c))}else o=EO(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function wO(t,n,e){let i=e?n.classBindings:n.styleBindings;if(aa(i)!==0)return t[os(i)]}function DO(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[os(r)]=i}function EO(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=t[o].hostAttrs;i=Gc(i,s,e)}return Gc(i,n.attrs,e)}function Ug(t,n,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=n[a],i=Gc(i,o.hostAttrs,r),o!==t);)a++;return t!==null&&(e.directiveStylingLast=a),i}function Gc(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),xu(t,s,e?!0:n[++o]))}return t===void 0?null:t}function SO(t,n,e){if(e==null||e==="")return rn;let i=[],r=vi(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function IO(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&xu(t,i,e)}function kO(t,n,e,i,r,o,s,a){r===an&&(r=rn);let c=0,l=0,u=0<r.length?r[0]:null,m=0<o.length?o[0]:null;for(;u!==null||m!==null;){let g=c<r.length?r[c+1]:void 0,_=l<o.length?o[l+1]:void 0,x=null,O;u===m?(c+=2,l+=2,g!==_&&(x=m,O=_)):m===null||u!==null&&u<m?(c+=2,x=u):(l+=2,x=m,O=_),x!==null&&Zw(t,n,e,i,x,O,s,a),u=c<r.length?r[c]:null,m=l<o.length?o[l]:null}}function Zw(t,n,e,i,r,o,s,a){if(!(n.type&3))return;let c=t.data,l=c[a+1],u=mO(l)?KC(c,n,e,r,aa(l),s):void 0;if(!cm(u)){cm(o)||dO(l)&&(o=KC(c,null,e,r,a,s));let m=mg(Ui(),e);bR(i,s,m,r,o)}}function KC(t,n,e,i,r,o){let s=n===null,a;for(;r>0;){let c=t[r],l=Array.isArray(c),u=l?c[1]:c,m=u===null,g=e[r+1];g===an&&(g=m?rn:void 0);let _=m?wu(g,i):u===i?g:void 0;if(l&&!cm(_)&&(_=wu(c,i)),cm(_)&&(a=_,s))return a;let x=t[r+1];r=s?os(x):aa(x)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(a=wu(c,i))}return a}function cm(t){return t!==void 0}function MO(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=Sc(vi(t)))),t}function Qw(t,n){return(t.flags&(n?8:16))!==0}function v(t,n=""){let e=ne(),i=Ze(),r=t+ot,o=i.firstCreatePass?la(i,r,1,n,null):i.data[r],s=TO(i,e,o,n);e[r]=s,Pu()&&Y_(i,e,s,o),Xs(o,!1)}var TO=(t,n,e,i)=>(Oc(!0),OA(n[Ue],i));function Xw(t,n,e,i=""){return _n(t,zi(),e)?n+qo(e)+i:an}function AO(t,n,e,i,r,o=""){let s=iC(),a=iv(t,s,e,r);return Tu(2),a?n+qo(e)+i+qo(r)+o:an}function Z(t){return De("",t),Z}function De(t,n,e){let i=ne(),r=Xw(i,t,n,e);return r!==an&&Kw(i,Ui(),r),De}function Rm(t,n,e,i,r){let o=ne(),s=AO(o,t,n,e,i,r);return s!==an&&Kw(o,Ui(),s),Rm}function Kw(t,n,e){let i=mg(n,t);NA(t[Ue],i,e)}function br(t,n,e){cv(n)&&(n=n());let i=ne(),r=zi();if(_n(i,r,n)){let o=Ze(),s=Js();tw(s,i,t,n,i[Ue],e)}return br}function to(t,n){let e=cv(t);return e&&t.set(n),e}function yr(t,n){let e=ne(),i=Ze(),r=Vt();return Uw(i,e,e[Ue],r,t,n),yr}function Xt(t){return _n(ne(),zi(),t)?qo(t):an}function Xn(t,n,e=""){return Xw(ne(),t,n,e)}function JC(t,n,e){let i=Ze();i.firstCreatePass&&Jw(n,i.data,i.blueprint,Hi(t),e)}function Jw(t,n,e,i,r){if(t=Qt(t),Array.isArray(t))for(let o=0;o<t.length;o++)Jw(t[o],n,e,i,r);else{let o=Ze(),s=ne(),a=Vt(),c=$o(t)?t:Qt(t.provide),l=sg(t),u=a.providerIndexes&1048575,m=a.directiveStart,g=a.providerIndexes>>20;if($o(t)||!t.multi){let _=new ns(l,r,ie,null),x=Gg(c,n,r?u:u+g,m);x===-1?(qg(Xu(a,s),o,c),$g(o,t,n.length),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(_),s.push(_)):(e[x]=_,s[x]=_)}else{let _=Gg(c,n,u+g,m),x=Gg(c,n,u,u+g),O=_>=0&&e[_],B=x>=0&&e[x];if(r&&!B||!r&&!O){qg(Xu(a,s),o,c);let q=NO(r?OO:RO,e.length,r,i,l,t);!r&&B&&(e[x].providerFactory=q),$g(o,t,n.length,0),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(q),s.push(q)}else{let q=eD(e[r?x:_],l,!r&&i);$g(o,t,_>-1?_:x,q)}!r&&i&&B&&e[x].componentProviders++}}}function $g(t,n,e,i){let r=$o(n),o=$0(n);if(r||o){let c=(o?Qt(n.useClass):n).prototype.ngOnDestroy;if(c){let l=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let u=l.indexOf(e);u===-1?l.push(e,[i,c]):l[u+1].push(i,c)}else l.push(e,c)}}}function eD(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function Gg(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function RO(t,n,e,i,r){return I_(this.multi,[])}function OO(t,n,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=Vc(i,i[oe],this.providerFactory.index,r);s=c.slice(0,a),I_(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],I_(o,s);return s}function I_(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function NO(t,n,e,i,r,o){let s=new ns(t,e,ie,null);return s.multi=[],s.index=n,s.componentProviders=0,eD(s,r,i&&!e),s}function Re(t,n){return e=>{e.providersResolver=(i,r)=>JC(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>JC(i,r?r(n):n,!0))}}function il(t,n){let e=Ks()+t,i=ne();return i[e]===an?wm(i,e,n()):u1(i,e)}function ma(t,n,e){return tD(ne(),Ks(),t,n,e)}function gv(t,n,e,i,r){return FO(ne(),Ks(),t,n,e,i,r)}function _v(t,n){let e=t[n];return e===an?void 0:e}function tD(t,n,e,i,r,o){let s=n+e;return _n(t,s,r)?wm(t,s+1,o?i.call(o,r):i(r)):_v(t,s+1)}function PO(t,n,e,i,r,o,s){let a=n+e;return iv(t,a,r,o)?wm(t,a+2,s?i.call(s,r,o):i(r,o)):_v(t,a+2)}function FO(t,n,e,i,r,o,s,a){let c=n+e;return m1(t,c,r,o,s)?wm(t,c+3,a?i.call(a,r,o,s):i(r,o,s)):_v(t,c+3)}function Ee(t,n){let e=Ze(),i,r=t+ot;e.firstCreatePass?(i=LO(n,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=Vr(i.type,!0)),s,a=hn(ie);try{let c=Qu(!1),l=o();return Qu(c),fg(e,ne(),r,l),l}finally{hn(a)}}function LO(t,n){if(n)for(let e=n.length-1;e>=0;e--){let i=n[e];if(t===i.name)return i}}function Sn(t,n,e){let i=t+ot,r=ne(),o=Su(r,i);return nD(r,i)?tD(r,Ks(),n,o.transform,e,o):o.transform(e)}function et(t,n,e,i){let r=t+ot,o=ne(),s=Su(o,r);return nD(o,r)?PO(o,Ks(),n,s.transform,e,i,s):s.transform(e,i)}function nD(t,n){return t[oe].data[n].pure}function Kn(t,n){return ym(t,n)}var lm=class{ngModuleFactory;componentFactories;constructor(n,e){this.ngModuleFactory=n,this.componentFactories=e}},vv=(()=>{class t{compileModuleSync(e){return new om(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}compileModuleAndAllComponentsSync(e){let i=this.compileModuleSync(e),r=Xp(e),o=Hx(r.declarations).reduce((s,a)=>{let c=ur(a);return c&&s.push(new rs(c)),s},[]);return new lm(i,o)}compileModuleAndAllComponentsAsync(e){return Promise.resolve(this.compileModuleAndAllComponentsSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var iD=(()=>{class t{applicationErrorHandler=d(Pn);appRef=d(En);taskService=d(pr);ngZone=d(j);zonelessEnabled=d(Nc);tracing=d(bi,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new ve;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Dc):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(Ng,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?_C:Tg;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Dc+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function rD(){return[{provide:Fi,useExisting:iD},{provide:j,useClass:Ec},{provide:Nc,useValue:!0}]}function BO(){return typeof $localize<"u"&&$localize.locale||nl}var fa=new y("",{factory:()=>d(fa,{optional:!0,skipSelf:!0})||BO()}),bv=new y("",{factory:()=>sO});var Om=class{destroyed=!1;listeners=null;errorHandler=d(on,{optional:!0});destroyRef=d(sn);constructor(){this.destroyRef.onDestroy(()=>{this.destroyed=!0,this.listeners=null})}subscribe(n){if(this.destroyed)throw new A(953,!1);return(this.listeners??=[]).push(n),{unsubscribe:()=>{let e=this.listeners?.indexOf(n);e!==void 0&&e!==-1&&this.listeners?.splice(e,1)}}}emit(n){if(this.destroyed){console.warn(Li(953,!1));return}if(this.listeners===null)return;let e=re(null);try{for(let i of this.listeners)try{i(n)}catch(r){this.errorHandler?.handleError(r)}}finally{re(e)}}};function Oe(t){return S0(t)}function Ct(t,n){return uc(t,n?.equal)}var jO=t=>t;function yv(t,n){if(typeof t=="function"){let e=Ap(t,jO,n?.equal);return oD(e,n?.debugName)}else{let e=Ap(t.source,t.computation,t.equal);return oD(e,t.debugName)}}function oD(t,n){let e=t[Dt],i=t;return i.set=r=>D0(e,r),i.update=r=>E0(e,r),i.asReadonly=Fu.bind(t),i}var uD=Symbol("InputSignalNode#UNSET"),XO=J(b({},mc),{transformFn:void 0,applyValueToInputSignal(t,n){Ao(t,n)}});function mD(t,n){let e=Object.create(XO);e.value=t,e.transformFn=n?.transform;function i(){if(Fr(e),e.value===uD){let r=null;throw new A(-950,r)}return e.value}return i[Dt]=e,i}var Jn=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>qc(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function fD(t){return new Om}function sD(t,n){return mD(t,n)}function KO(t){return mD(uD,t)}var ha=(sD.required=KO,sD);function aD(t,n){return sv(n)}function JO(t,n){return av(n)}var ol=(aD.required=JO,aD);function cD(t,n){return sv(n)}function eN(t,n){return av(n)}var hD=(cD.required=eN,cD);var xv=new y(""),tN=new y("");function rl(t){return!t.moduleRef}function nN(t){let n=rl(t)?t.r3Injector:t.moduleRef.injector,e=n.get(j);return e.run(()=>{rl(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(Pn),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),rl(t)){let o=()=>n.destroy(),s=t.platformInjector.get(xv);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>t.moduleRef.destroy(),s=t.platformInjector.get(xv);s.add(o),t.moduleRef.onDestroy(()=>{jc(t.allPlatformModules,t.moduleRef),r.unsubscribe(),s.delete(o)})}return rN(i,e,()=>{let o=n.get(pr),s=o.add(),a=n.get(mv);return a.runInitializers(),a.donePromise.then(()=>{let c=n.get(fa,nl);if(zw(c||nl),!n.get(tN,!0))return rl(t)?n.get(En):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(rl(t)){let u=n.get(En);return t.rootComponent!==void 0&&u.bootstrap(t.rootComponent),u}else return iN?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(s)})})})}var iN;function rN(t,n,e){try{let i=e();return Jr(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var Nm=null;function oN(t=[],n){return X.create({name:n,providers:[{provide:Mc,useValue:"platform"},{provide:xv,useValue:new Set([()=>Nm=null])},...t]})}function sN(t=[]){if(Nm)return Nm;let n=oN(t);return Nm=n,jw(),aN(n),n}function aN(t){let n=t.get(dm,null);Gt(t,()=>{n?.forEach(e=>e())})}var cN=1e4;var GZ=cN-1e3;var ue=(()=>{class t{static __NG_ELEMENT_ID__=lN}return t})();function lN(t){return dN(Vt(),ne(),(t&16)===16)}function dN(t,n,e){if(Vi(t)&&!e){let i=Yn(t.index,n);return new Zr(i,i)}else if(t.type&175){let i=n[gn];return new Zr(i,n)}return null}function pD(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;Le(Ae.BootstrapApplicationStart);try{let o=r?.injector??sN(i),s=[rD(),bC,...e||[]],a=new $c({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return nN({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{Le(Ae.BootstrapApplicationEnd)}}function V(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function ut(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var Cv=Symbol("NOT_SET"),gD=new Set,uN=J(b({},mc),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Cv,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Cv&&!Fs(this))return this.signal;try{for(let r of this.cleanup??gD)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=cr(this),i;try{i=this.userFn.apply(null,n)}finally{Lr(this,e)}return(this.value===Cv||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),wv=class extends Hc{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(sn),s),this.scheduler=r;for(let a of G_){let c=e[a];if(c===void 0)continue;let l=Object.create(uN);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(Fr(l),l.value),l.signal[Dt]=l,l.registerCleanupFn=u=>(l.cleanup??=new Set).add(u),this.nodes[a]=l,this.hooks[a]=u=>l.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??gD)e()}finally{Br(n)}}};function _D(t,n){let e=n?.injector??d(X),i=e.get(Fi),r=e.get(pm),o=e.get(bi,null,{optional:!0});r.impl??=e.get(W_);let s=t;typeof s=="function"&&(s={mixedReadWrite:t});let a=e.get(ea,null,{optional:!0}),c=new wv(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(c),c}function Pm(t,n){let e=ur(t),i=n.elementInjector||Ws();return new rs(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var vD=null;function ei(){return vD}function Dv(t){vD??=t}var sl=class{},pa=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(bD),providedIn:"platform"})}return t})();var bD=(()=>{class t extends pa{_location;_history;_doc=d(K);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return ei().getBaseHref(this._doc)}onPopState(e){let i=ei().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=ei().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function xD(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function yD(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function no(t){return t&&t[0]!=="?"?`?${t}`:t}var ga=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(fN),providedIn:"root"})}return t})(),mN=new y(""),fN=(()=>{class t extends ga{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(K).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return xD(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+no(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+no(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+no(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(te(pa),te(mN,8))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Zi=(()=>{class t{_subject=new I;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=gN(yD(CD(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+no(i))}normalize(e){return t.stripTrailingSlash(pN(this._basePath,CD(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+no(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+no(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=no;static joinWithSlash=xD;static stripTrailingSlash=yD;static \u0275fac=function(i){return new(i||t)(te(ga))};static \u0275prov=C({token:t,factory:()=>hN(),providedIn:"root"})}return t})();function hN(){return new Zi(te(ga))}function pN(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function CD(t){return t.replace(/\/index.html$/,"")}function gN(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var ID={ADP:[void 0,void 0,0],AFN:[void 0,"\u060B",0],ALL:[void 0,void 0,0],AMD:[void 0,"\u058F",2],AOA:[void 0,"Kz"],ARS:[void 0,"$"],AUD:["A$","$"],AZN:[void 0,"\u20BC"],BAM:[void 0,"KM"],BBD:[void 0,"$"],BDT:[void 0,"\u09F3"],BHD:[void 0,void 0,3],BIF:[void 0,void 0,0],BMD:[void 0,"$"],BND:[void 0,"$"],BOB:[void 0,"Bs"],BRL:["R$"],BSD:[void 0,"$"],BWP:[void 0,"P"],BYN:[void 0,void 0,2],BYR:[void 0,void 0,0],BZD:[void 0,"$"],CAD:["CA$","$",2],CHF:[void 0,void 0,2],CLF:[void 0,void 0,4],CLP:[void 0,"$",0],CNY:["CN\xA5","\xA5"],COP:[void 0,"$",2],CRC:[void 0,"\u20A1",2],CUC:[void 0,"$"],CUP:[void 0,"$"],CZK:[void 0,"K\u010D",2],DJF:[void 0,void 0,0],DKK:[void 0,"kr",2],DOP:[void 0,"$"],EGP:[void 0,"E\xA3"],ESP:[void 0,"\u20A7",0],EUR:["\u20AC"],FJD:[void 0,"$"],FKP:[void 0,"\xA3"],GBP:["\xA3"],GEL:[void 0,"\u20BE"],GHS:[void 0,"GH\u20B5"],GIP:[void 0,"\xA3"],GNF:[void 0,"FG",0],GTQ:[void 0,"Q"],GYD:[void 0,"$",2],HKD:["HK$","$"],HNL:[void 0,"L"],HRK:[void 0,"kn"],HUF:[void 0,"Ft",2],IDR:[void 0,"Rp",2],ILS:["\u20AA"],INR:["\u20B9"],IQD:[void 0,void 0,0],IRR:[void 0,void 0,0],ISK:[void 0,"kr",0],ITL:[void 0,void 0,0],JMD:[void 0,"$"],JOD:[void 0,void 0,3],JPY:["\xA5",void 0,0],KGS:[void 0,"\u20C0"],KHR:[void 0,"\u17DB"],KMF:[void 0,"CF",0],KPW:[void 0,"\u20A9",0],KRW:["\u20A9",void 0,0],KWD:[void 0,void 0,3],KYD:[void 0,"$"],KZT:[void 0,"\u20B8"],LAK:[void 0,"\u20AD",0],LBP:[void 0,"L\xA3",0],LKR:[void 0,"Rs"],LRD:[void 0,"$"],LTL:[void 0,"Lt"],LUF:[void 0,void 0,0],LVL:[void 0,"Ls"],LYD:[void 0,void 0,3],MGA:[void 0,"Ar",0],MGF:[void 0,void 0,0],MMK:[void 0,"K",0],MNT:[void 0,"\u20AE",2],MRO:[void 0,void 0,0],MUR:[void 0,"Rs",2],MXN:["MX$","$"],MYR:[void 0,"RM"],NAD:[void 0,"$"],NGN:[void 0,"\u20A6"],NIO:[void 0,"C$"],NOK:[void 0,"kr",2],NPR:[void 0,"Rs"],NZD:["NZ$","$"],OMR:[void 0,void 0,3],PHP:["\u20B1"],PKR:[void 0,"Rs",2],PLN:[void 0,"z\u0142"],PYG:[void 0,"\u20B2",0],RON:[void 0,"lei"],RSD:[void 0,void 0,0],RUB:[void 0,"\u20BD"],RWF:[void 0,"RF",0],SBD:[void 0,"$"],SEK:[void 0,"kr",2],SGD:[void 0,"$"],SHP:[void 0,"\xA3"],SLE:[void 0,void 0,2],SLL:[void 0,void 0,0],SOS:[void 0,void 0,0],SRD:[void 0,"$"],SSP:[void 0,"\xA3"],STD:[void 0,void 0,0],STN:[void 0,"Db"],SYP:[void 0,"\xA3",0],THB:[void 0,"\u0E3F"],TMM:[void 0,void 0,0],TND:[void 0,void 0,3],TOP:[void 0,"T$"],TRL:[void 0,void 0,0],TRY:[void 0,"\u20BA"],TTD:[void 0,"$"],TWD:["NT$","$",2],TZS:[void 0,void 0,2],UAH:[void 0,"\u20B4"],UGX:[void 0,void 0,0],USD:["$"],UYI:[void 0,void 0,0],UYU:[void 0,"$"],UYW:[void 0,void 0,4],UZS:[void 0,void 0,2],VEF:[void 0,"Bs",2],VND:["\u20AB",void 0,0],VUV:[void 0,void 0,0],XAF:["FCFA",void 0,0],XCD:["EC$","$"],XCG:["Cg."],XOF:["F\u202FCFA",void 0,0],XPF:["CFPF",void 0,0],XXX:["\xA4"],YER:[void 0,void 0,0],ZAR:[void 0,"R"],ZMK:[void 0,void 0,0],ZMW:[void 0,"ZK"],ZWD:[void 0,void 0,0]},Tv=(function(t){return t[t.Decimal=0]="Decimal",t[t.Percent=1]="Percent",t[t.Currency=2]="Currency",t[t.Scientific=3]="Scientific",t})(Tv||{});var ln=(function(t){return t[t.Format=0]="Format",t[t.Standalone=1]="Standalone",t})(ln||{}),We=(function(t){return t[t.Narrow=0]="Narrow",t[t.Abbreviated=1]="Abbreviated",t[t.Wide=2]="Wide",t[t.Short=3]="Short",t})(We||{}),In=(function(t){return t[t.Short=0]="Short",t[t.Medium=1]="Medium",t[t.Long=2]="Long",t[t.Full=3]="Full",t})(In||{}),kn={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function kD(t){return vn(t)[ht.LocaleId]}function MD(t,n,e){let i=vn(t),r=[i[ht.DayPeriodsFormat],i[ht.DayPeriodsStandalone]],o=ti(r,n);return ti(o,e)}function TD(t,n,e){let i=vn(t),r=[i[ht.DaysFormat],i[ht.DaysStandalone]],o=ti(r,n);return ti(o,e)}function AD(t,n,e){let i=vn(t),r=[i[ht.MonthsFormat],i[ht.MonthsStandalone]],o=ti(r,n);return ti(o,e)}function RD(t,n){let i=vn(t)[ht.Eras];return ti(i,n)}function al(t,n){let e=vn(t);return ti(e[ht.DateFormat],n)}function cl(t,n){let e=vn(t);return ti(e[ht.TimeFormat],n)}function ll(t,n){let i=vn(t)[ht.DateTimeFormat];return ti(i,n)}function Qi(t,n){let e=vn(t),i=e[ht.NumberSymbols][n];if(typeof i>"u"){if(n===kn.CurrencyDecimal)return e[ht.NumberSymbols][kn.Decimal];if(n===kn.CurrencyGroup)return e[ht.NumberSymbols][kn.Group]}return i}function OD(t,n){return vn(t)[ht.NumberFormats][n]}function vN(t){return vn(t)[ht.Currencies]}function ND(t){if(!t[ht.ExtraData])throw new A(2303,!1)}function PD(t){let n=vn(t);return ND(n),(n[ht.ExtraData][2]||[]).map(i=>typeof i=="string"?Ev(i):[Ev(i[0]),Ev(i[1])])}function FD(t,n,e){let i=vn(t);ND(i);let r=[i[ht.ExtraData][0],i[ht.ExtraData][1]],o=ti(r,n)||[];return ti(o,e)||[]}function ti(t,n){for(let e=n;e>-1;e--)if(typeof t[e]<"u")return t[e];throw new A(2304,!1)}function Ev(t){let[n,e]=t.split(":");return{hours:+n,minutes:+e}}function LD(t,n,e="en"){let i=vN(e)[t]||ID[t]||[],r=i[1];return n==="narrow"&&typeof r=="string"?r:i[0]||t}var bN=2;function BD(t){let n,e=ID[t];return e&&(n=e[2]),typeof n=="number"?n:bN}var yN=/^(\d{4,})-?(\d\d)-?(\d\d)(?:T(\d\d)(?::?(\d\d)(?::?(\d\d)(?:\.(\d+))?)?)?(Z|([+-])(\d\d):?(\d\d))?)?$/,Fm={},CN=/((?:[^BEGHLMOSWYZabcdhmswyz']+)|(?:'(?:[^']|'')*')|(?:G{1,5}|y{1,4}|Y{1,4}|M{1,5}|L{1,5}|w{1,2}|W{1}|d{1,2}|E{1,6}|c{1,6}|a{1,5}|b{1,5}|B{1,5}|h{1,2}|H{1,2}|m{1,2}|s{1,2}|S{1,3}|z{1,4}|Z{1,5}|O{1,4}))([\s\S]*)/;function jD(t,n,e,i){let r=TN(t);n=Cr(e,n)||n;let s=[],a;for(;n;)if(a=CN.exec(n),a){s=s.concat(a.slice(1));let u=s.pop();if(!u)break;n=u}else{s.push(n);break}let c=r.getTimezoneOffset();i&&(c=HD(i,c),r=MN(r,i));let l="";return s.forEach(u=>{let m=IN(u);l+=m?m(r,e,c):u==="''"?"'":u.replace(/(^'|'$)/g,"").replace(/''/g,"'")}),l}function Hm(t,n,e){let i=new Date(0);return i.setFullYear(t,n,e),i.setHours(0,0,0),i}function Cr(t,n){let e=kD(t);if(Fm[e]??={},Fm[e][n])return Fm[e][n];let i="";switch(n){case"shortDate":i=al(t,In.Short);break;case"mediumDate":i=al(t,In.Medium);break;case"longDate":i=al(t,In.Long);break;case"fullDate":i=al(t,In.Full);break;case"shortTime":i=cl(t,In.Short);break;case"mediumTime":i=cl(t,In.Medium);break;case"longTime":i=cl(t,In.Long);break;case"fullTime":i=cl(t,In.Full);break;case"short":let r=Cr(t,"shortTime"),o=Cr(t,"shortDate");i=Lm(ll(t,In.Short),[r,o]);break;case"medium":let s=Cr(t,"mediumTime"),a=Cr(t,"mediumDate");i=Lm(ll(t,In.Medium),[s,a]);break;case"long":let c=Cr(t,"longTime"),l=Cr(t,"longDate");i=Lm(ll(t,In.Long),[c,l]);break;case"full":let u=Cr(t,"fullTime"),m=Cr(t,"fullDate");i=Lm(ll(t,In.Full),[u,m]);break}return i&&(Fm[e][n]=i),i}function Lm(t,n){return n&&(t=t.replace(/\{([^}]+)}/g,function(e,i){return n!=null&&i in n?n[i]:e})),t}function yi(t,n,e="-",i,r){let o="";(t<0||r&&t<=0)&&(r?t=-t+1:(t=-t,o=e));let s=String(t);for(;s.length<n;)s="0"+s;return i&&(s=s.slice(s.length-n)),o+s}function xN(t,n){return yi(t,3).substring(0,n)}function Ft(t,n,e=0,i=!1,r=!1){return function(o,s){let a=wN(t,o);if((e>0||a>-e)&&(a+=e),t===3)a===0&&e===-12&&(a=12);else if(t===6)return xN(a,n);let c=Qi(s,kn.MinusSign);return yi(a,n,c,i,r)}}function wN(t,n){switch(t){case 0:return n.getFullYear();case 1:return n.getMonth();case 2:return n.getDate();case 3:return n.getHours();case 4:return n.getMinutes();case 5:return n.getSeconds();case 6:return n.getMilliseconds();case 7:return n.getDay();default:throw new A(2301,!1)}}function tt(t,n,e=ln.Format,i=!1){return function(r,o){return DN(r,o,t,n,e,i)}}function DN(t,n,e,i,r,o){switch(e){case 2:return AD(n,r,i)[t.getMonth()];case 1:return TD(n,r,i)[t.getDay()];case 0:let s=t.getHours(),a=t.getMinutes();if(o){let l=PD(n),u=FD(n,r,i),m=l.findIndex(g=>{if(Array.isArray(g)){let[_,x]=g,O=s>=_.hours&&a>=_.minutes,B=s<x.hours||s===x.hours&&a<x.minutes;if(_.hours<x.hours){if(O&&B)return!0}else if(O||B)return!0}else if(g.hours===s&&g.minutes===a)return!0;return!1});if(m!==-1)return u[m]}return MD(n,r,i)[s<12?0:1];case 3:return RD(n,i)[t.getFullYear()<=0?0:1];default:let c=e;throw new A(2302,!1)}}function Bm(t){return function(n,e,i){let r=-1*i,o=Qi(e,kn.MinusSign),s=r>0?Math.floor(r/60):Math.ceil(r/60);switch(t){case 0:return(r>=0?"+":"")+yi(s,2,o)+yi(Math.abs(r%60),2,o);case 1:return"GMT"+(r>=0?"+":"")+yi(s,1,o);case 2:return"GMT"+(r>=0?"+":"")+yi(s,2,o)+":"+yi(Math.abs(r%60),2,o);case 3:return i===0?"Z":(r>=0?"+":"")+yi(s,2,o)+":"+yi(Math.abs(r%60),2,o);default:throw new A(2310,!1)}}}var EN=0,Vm=4;function SN(t){let n=Hm(t,EN,1).getDay();return Hm(t,0,1+(n<=Vm?Vm:Vm+7)-n)}function VD(t){let n=t.getDay(),e=n===0?-3:Vm-n;return Hm(t.getFullYear(),t.getMonth(),t.getDate()+e)}function Sv(t,n=!1){return function(e,i){let r;if(n){let o=new Date(e.getFullYear(),e.getMonth(),1).getDay()-1,s=e.getDate();r=1+Math.floor((s+o)/7)}else{let o=VD(e),s=SN(o.getFullYear()),a=o.getTime()-s.getTime();r=1+Math.round(a/6048e5)}return yi(r,t,Qi(i,kn.MinusSign))}}function jm(t,n=!1){return function(e,i){let o=VD(e).getFullYear();return yi(o,t,Qi(i,kn.MinusSign),n)}}var Iv={};function IN(t){if(Iv[t])return Iv[t];let n;switch(t){case"G":case"GG":case"GGG":n=tt(3,We.Abbreviated);break;case"GGGG":n=tt(3,We.Wide);break;case"GGGGG":n=tt(3,We.Narrow);break;case"y":n=Ft(0,1,0,!1,!0);break;case"yy":n=Ft(0,2,0,!0,!0);break;case"yyy":n=Ft(0,3,0,!1,!0);break;case"yyyy":n=Ft(0,4,0,!1,!0);break;case"Y":n=jm(1);break;case"YY":n=jm(2,!0);break;case"YYY":n=jm(3);break;case"YYYY":n=jm(4);break;case"M":case"L":n=Ft(1,1,1);break;case"MM":case"LL":n=Ft(1,2,1);break;case"MMM":n=tt(2,We.Abbreviated);break;case"MMMM":n=tt(2,We.Wide);break;case"MMMMM":n=tt(2,We.Narrow);break;case"LLL":n=tt(2,We.Abbreviated,ln.Standalone);break;case"LLLL":n=tt(2,We.Wide,ln.Standalone);break;case"LLLLL":n=tt(2,We.Narrow,ln.Standalone);break;case"w":n=Sv(1);break;case"ww":n=Sv(2);break;case"W":n=Sv(1,!0);break;case"d":n=Ft(2,1);break;case"dd":n=Ft(2,2);break;case"c":case"cc":n=Ft(7,1);break;case"ccc":n=tt(1,We.Abbreviated,ln.Standalone);break;case"cccc":n=tt(1,We.Wide,ln.Standalone);break;case"ccccc":n=tt(1,We.Narrow,ln.Standalone);break;case"cccccc":n=tt(1,We.Short,ln.Standalone);break;case"E":case"EE":case"EEE":n=tt(1,We.Abbreviated);break;case"EEEE":n=tt(1,We.Wide);break;case"EEEEE":n=tt(1,We.Narrow);break;case"EEEEEE":n=tt(1,We.Short);break;case"a":case"aa":case"aaa":n=tt(0,We.Abbreviated);break;case"aaaa":n=tt(0,We.Wide);break;case"aaaaa":n=tt(0,We.Narrow);break;case"b":case"bb":case"bbb":n=tt(0,We.Abbreviated,ln.Standalone,!0);break;case"bbbb":n=tt(0,We.Wide,ln.Standalone,!0);break;case"bbbbb":n=tt(0,We.Narrow,ln.Standalone,!0);break;case"B":case"BB":case"BBB":n=tt(0,We.Abbreviated,ln.Format,!0);break;case"BBBB":n=tt(0,We.Wide,ln.Format,!0);break;case"BBBBB":n=tt(0,We.Narrow,ln.Format,!0);break;case"h":n=Ft(3,1,-12);break;case"hh":n=Ft(3,2,-12);break;case"H":n=Ft(3,1);break;case"HH":n=Ft(3,2);break;case"m":n=Ft(4,1);break;case"mm":n=Ft(4,2);break;case"s":n=Ft(5,1);break;case"ss":n=Ft(5,2);break;case"S":n=Ft(6,1);break;case"SS":n=Ft(6,2);break;case"SSS":n=Ft(6,3);break;case"Z":case"ZZ":case"ZZZ":n=Bm(0);break;case"ZZZZZ":n=Bm(3);break;case"O":case"OO":case"OOO":case"z":case"zz":case"zzz":n=Bm(1);break;case"OOOO":case"ZZZZ":case"zzzz":n=Bm(2);break;default:return null}return Iv[t]=n,n}function HD(t,n){t=t.replace(/:/g,"");let e=Date.parse("Jan 01, 1970 00:00:00 "+t)/6e4;return isNaN(e)?n:e}function kN(t,n){return t=new Date(t.getTime()),t.setMinutes(t.getMinutes()+n),t}function MN(t,n,e){let r=t.getTimezoneOffset(),o=HD(n,r);return kN(t,-1*(o-r))}function TN(t){if(wD(t))return t;if(typeof t=="number"&&!isNaN(t))return new Date(t);if(typeof t=="string"){if(t=t.trim(),/^(\d{4}(-\d{1,2}(-\d{1,2})?)?)$/.test(t)){let[r,o=1,s=1]=t.split("-").map(a=>+a);return Hm(r,o-1,s)}let e=parseFloat(t);if(!isNaN(t-e))return new Date(e);let i;if(i=t.match(yN))return AN(i)}let n=new Date(t);if(!wD(n))throw new A(2311,!1);return n}function AN(t){let n=new Date(0),e=0,i=0,r=t[8]?n.setUTCFullYear:n.setFullYear,o=t[8]?n.setUTCHours:n.setHours;t[9]&&(e=Number(t[9]+t[10]),i=Number(t[9]+t[11])),r.call(n,Number(t[1]),Number(t[2])-1,Number(t[3]));let s=Number(t[4]||0)-e,a=Number(t[5]||0)-i,c=Number(t[6]||0),l=Math.floor(parseFloat("0."+(t[7]||0))*1e3);return o.call(n,s,a,c,l),n}function wD(t){return t instanceof Date&&!isNaN(t.valueOf())}var RN=/^(\d+)?\.((\d+)(-(\d+))?)?$/,DD=22,zm=".",dl="0",ON=";",NN=",",kv="#",ED="\xA4";function PN(t,n,e,i,r,o,s=!1){let a="",c=!1;if(!isFinite(t))a=Qi(e,kn.Infinity);else{let l=BN(t);s&&(l=LN(l));let u=n.minInt,m=n.minFrac,g=n.maxFrac;if(o){let be=o.match(RN);if(be===null)throw new A(2306,!1);let wt=be[1],gt=be[3],Nr=be[5];wt!=null&&(u=Mv(wt)),gt!=null&&(m=Mv(gt)),Nr!=null?g=Mv(Nr):gt!=null&&m>g&&(g=m)}jN(l,m,g);let _=l.digits,x=l.integerLen,O=l.exponent,B=[];for(c=_.every(be=>!be);x<u;x++)_.unshift(0);for(;x<0;x++)_.unshift(0);x>0?B=_.splice(x,_.length):(B=_,_=[0]);let q=[];for(_.length>=n.lgSize&&q.unshift(_.splice(-n.lgSize,_.length).join(""));_.length>n.gSize;)q.unshift(_.splice(-n.gSize,_.length).join(""));_.length&&q.unshift(_.join("")),a=q.join(Qi(e,i)),B.length&&(a+=Qi(e,r)+B.join("")),O&&(a+=Qi(e,kn.Exponential)+"+"+O)}return t<0&&!c?a=n.negPre+a+n.negSuf:a=n.posPre+a+n.posSuf,a}function zD(t,n,e,i,r){let o=OD(n,Tv.Currency),s=FN(o,Qi(n,kn.MinusSign));return s.minFrac=BD(i),s.maxFrac=s.minFrac,PN(t,s,n,kn.CurrencyGroup,kn.CurrencyDecimal,r).replace(ED,e).replace(ED,"").trim()}function FN(t,n="-"){let e={minInt:1,minFrac:0,maxFrac:0,posPre:"",posSuf:"",negPre:"",negSuf:"",gSize:0,lgSize:0},i=t.split(ON),r=i[0],o=i[1],s=r.indexOf(zm)!==-1?r.split(zm):[r.substring(0,r.lastIndexOf(dl)+1),r.substring(r.lastIndexOf(dl)+1)],a=s[0],c=s[1]||"";e.posPre=a.substring(0,a.indexOf(kv));for(let u=0;u<c.length;u++){let m=c.charAt(u);m===dl?e.minFrac=e.maxFrac=u+1:m===kv?e.maxFrac=u+1:e.posSuf+=m}let l=a.split(NN);if(e.gSize=l[1]?l[1].length:0,e.lgSize=l[2]||l[1]?(l[2]||l[1]).length:0,o){let u=r.length-e.posPre.length-e.posSuf.length,m=o.indexOf(kv);e.negPre=o.substring(0,m).replace(/'/g,""),e.negSuf=o.slice(m+u).replace(/'/g,"")}else e.negPre=n+e.posPre,e.negSuf=e.posSuf;return e}function LN(t){if(t.digits[0]===0)return t;let n=t.digits.length-t.integerLen;return t.exponent?t.exponent+=2:(n===0?t.digits.push(0,0):n===1&&t.digits.push(0),t.integerLen+=2),t}function BN(t){let n=Math.abs(t)+"",e=0,i,r,o,s,a;for((r=n.indexOf(zm))>-1&&(n=n.replace(zm,"")),(o=n.search(/e/i))>0?(r<0&&(r=o),r+=+n.slice(o+1),n=n.substring(0,o)):r<0&&(r=n.length),o=0;n.charAt(o)===dl;o++);if(o===(a=n.length))i=[0],r=1;else{for(a--;n.charAt(a)===dl;)a--;for(r-=o,i=[],s=0;o<=a;o++,s++)i[s]=Number(n.charAt(o))}return r>DD&&(i=i.splice(0,DD-1),e=r-1,r=1),{digits:i,exponent:e,integerLen:r}}function jN(t,n,e){if(n>e)throw new A(2307,!1);let i=t.digits,r=i.length-t.integerLen,o=Math.min(Math.max(n,r),e),s=o+t.integerLen,a=i[s];if(s>0){i.splice(Math.max(t.integerLen,s));for(let m=s;m<i.length;m++)i[m]=0}else{r=Math.max(0,r),t.integerLen=1,i.length=Math.max(1,s=o+1),i[0]=0;for(let m=1;m<s;m++)i[m]=0}if(a>=5)if(s-1<0){for(let m=0;m>s;m--)i.unshift(0),t.integerLen++;i.unshift(1),t.integerLen++}else i[s-1]++;for(;r<Math.max(0,o);r++)i.push(0);let c=o!==0,l=n+t.integerLen,u=i.reduceRight(function(m,g,_,x){return g=g+m,x[_]=g<10?g:g-10,c&&(x[_]===0&&_>=l?x.pop():c=!1),g>=10?1:0},0);u&&(i.unshift(u),t.integerLen++)}function Mv(t){let n=parseInt(t);if(isNaN(n))throw new A(2305,!1);return n}var Av=(()=>{class t{_viewContainer;_context=new Um;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(e,i){this._viewContainer=e,this._thenTemplateRef=i}set ngIf(e){this._context.$implicit=this._context.ngIf=e,this._updateView()}set ngIfThen(e){SD(e,!1),this._thenTemplateRef=e,this._thenViewRef=null,this._updateView()}set ngIfElse(e){SD(e,!1),this._elseTemplateRef=e,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(ie(Nt),ie(mt))};static \u0275dir=T({type:t,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return t})(),Um=class{$implicit=null;ngIf=null};function SD(t,n){if(t&&!t.createEmbeddedView)throw new A(2020,!1)}var Xi=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(X);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(ie(Nt))};static \u0275dir=T({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[$e]})}return t})();function UD(t,n){return new A(2100,!1)}var VN="mediumDate",$D=new y(""),GD=new y(""),ls=(()=>{class t{locale;defaultTimezone;defaultOptions;constructor(e,i,r){this.locale=e,this.defaultTimezone=i,this.defaultOptions=r}transform(e,i,r,o){if(e==null||e===""||e!==e)return null;try{let s=i??this.defaultOptions?.dateFormat??VN,a=r??this.defaultOptions?.timezone??this.defaultTimezone??void 0;return jD(e,s,o||this.locale,a)}catch(s){throw UD(t,s.message)}}static \u0275fac=function(i){return new(i||t)(ie(fa,16),ie($D,24),ie(GD,24))};static \u0275pipe=Kr({name:"date",type:t,pure:!0})}return t})();var kt=(()=>{class t{_locale;_defaultCurrencyCode;constructor(e,i="USD"){this._locale=e,this._defaultCurrencyCode=i}transform(e,i=this._defaultCurrencyCode,r="symbol",o,s){if(!HN(e))return null;s||=this._locale,typeof r=="boolean"&&(r=r?"symbol":"code");let a=i||this._defaultCurrencyCode;r!=="code"&&(r==="symbol"||r==="symbol-narrow"?a=LD(a,r==="symbol"?"wide":"narrow",s):a=r);try{let c=zN(e);return zD(c,s,a,i,o)}catch(c){throw UD(t,c.message)}}static \u0275fac=function(i){return new(i||t)(ie(fa,16),ie(bv,16))};static \u0275pipe=Kr({name:"currency",type:t,pure:!0})}return t})();function HN(t){return!(t==null||t===""||t!==t)}function zN(t){if(typeof t=="string"&&!isNaN(Number(t)-parseFloat(t)))return Number(t);if(typeof t!="number")throw new A(2309,!1);return t}var $m=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({})}return t})();function ul(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var ds=class{};var Rv="browser";function WD(t){return t===Rv}var ml=class{_doc;constructor(n){this._doc=n}manager},Gm=(()=>{class t extends ml{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(te(K))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),Ym=new y(""),Fv=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof Gm));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof Gm);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new A(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(te(Ym),te(j))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),Ov="ng-app-id";function YD(t){for(let n of t)n.remove()}function ZD(t,n){let e=n.createElement("style");return e.textContent=t,e}function $N(t,n,e,i){let r=t.head?.querySelectorAll(`style[${Ov}="${n}"],link[${Ov}="${n}"]`);if(r)for(let o of r)o.removeAttribute(Ov),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]})}function Pv(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Lv=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,$N(e,i,this.inline,this.external),this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,ZD);i?.forEach(r=>this.addUsage(r,this.external,Pv))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(YD(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])YD(e);this.hosts.clear()}addHost(e){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,ZD(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,Pv(i,this.doc)))}removeHost(e){this.hosts.delete(e)}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(te(K),te(Qr),te(as,8),te(ss))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),Nv={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Bv=/%COMP%/g;var XD="%COMP%",GN=`_nghost-${XD}`,WN=`_ngcontent-${XD}`,qN=!0,YN=new y("",{factory:()=>qN});function ZN(t){return WN.replace(Bv,t)}function QN(t){return GN.replace(Bv,t)}function KD(t,n){return n.map(e=>e.replace(Bv,t))}var jv=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,s,a,c=null,l=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new fl(e,s,a,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof qm?r.applyToHost(e):r instanceof hl&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,m=this.tracingService;switch(i.encapsulation){case _i.Emulated:o=new qm(c,l,i,this.appId,u,s,a,m);break;case _i.ShadowDom:return new Wm(c,e,i,s,a,this.nonce,m,l);case _i.ExperimentalIsolatedShadowDom:return new Wm(c,e,i,s,a,this.nonce,m);default:o=new hl(c,l,i,u,s,a,m);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(te(Fv),te(Lv),te(Qr),te(YN),te(K),te(j),te(as),te(bi,8))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),fl=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Nv[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(QD(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(QD(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new A(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Nv[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Nv[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(Wi.DashCase|Wi.Important)?n.style.setProperty(e,i,r&Wi.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&Wi.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=ei().getGlobalEventTarget(this.doc,n),!n))throw new A(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function QD(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Wm=class extends fl{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,s,a,c){super(n,r,o,a),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=KD(i.id,l);for(let m of l){let g=document.createElement("style");s&&g.setAttribute("nonce",s),g.textContent=m,this.shadowRoot.appendChild(g)}let u=i.getExternalStyles?.();if(u)for(let m of u){let g=Pv(m,r);s&&g.setAttribute("nonce",s),this.shadowRoot.appendChild(g)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},hl=class extends fl{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,s,a,c){super(n,o,s,a),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?KD(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&is.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},qm=class extends hl{contentAttr;hostAttr;constructor(n,e,i,r,o,s,a,c){let l=r+"-"+i.id;super(n,e,i,o,s,a,c,l),this.contentAttr=ZN(l),this.hostAttr=QN(l)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Zm=class t extends sl{supportsDOMEvents=!0;static makeCurrent(){Dv(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=XN();return e==null?null:KN(e)}resetBaseElement(){pl=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return ul(document.cookie,n)}},pl=null;function XN(){return pl=pl||document.head.querySelector("base"),pl?pl.getAttribute("href"):null}function KN(t){return new URL(t,document.baseURI).pathname}var JN=(()=>{class t{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),JD=["alt","control","meta","shift"],eP={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},tP={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},eE=(()=>{class t extends ml{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=t.parseEventName(i),a=t.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>ei().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),JD.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(e,i){let r=eP[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),JD.forEach(s=>{if(s!==r){let a=tP[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(te(K))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})();async function Vv(t,n,e){let i=b({rootComponent:t},nP(n,e));return pD(i)}function nP(t,n){return{platformRef:n?.platformRef,appProviders:[...aP,...t?.providers??[]],platformProviders:sP}}function iP(){Zm.makeCurrent()}function rP(){return new on}function oP(){return T_(document),document}var sP=[{provide:ss,useValue:Rv},{provide:dm,useValue:iP,multi:!0},{provide:K,useFactory:oP}];var aP=[{provide:Mc,useValue:"root"},{provide:on,useFactory:rP},{provide:Ym,useClass:Gm,multi:!0},{provide:Ym,useClass:eE,multi:!0},jv,Lv,Fv,{provide:Ot,useExisting:jv},{provide:ds,useClass:JN},[]];var io=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=this.headers.get(e);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var Xm=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},Km=class{encodeKey(n){return tE(n)}encodeValue(n){return tE(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function cP(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],c=e.get(s)||[];c.push(a),e.set(s,c)}),e}var lP=/%(\d[a-f0-9])/gi,dP={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function tE(t){return encodeURIComponent(t).replace(lP,(n,e)=>dP[e]??n)}function Qm(t){return`${t}`}var Fn=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new Km,n.fromString){if(n.fromObject)throw new A(2805,!1);this.map=cP(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(Qm):[Qm(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(Qm(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(Qm(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function uP(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function nE(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function iE(t){return typeof Blob<"u"&&t instanceof Blob}function rE(t){return typeof FormData<"u"&&t instanceof FormData}function mP(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var oE="Content-Type",sE="Accept",aE="text/plain",cE="application/json",fP=`${cE}, ${aE}, */*`,_a=class t{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(uP(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new A(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new io,this.context??=new Xm,!this.params)this.params=new Fn,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e.indexOf("?"),c=a===-1?"?":a<e.length-1?"&":"";this.urlWithParams=e+c+s}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||nE(this.body)||iE(this.body)||rE(this.body)||mP(this.body)?this.body:this.body instanceof Fn?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||rE(this.body)?null:iE(this.body)?this.body.type||null:nE(this.body)?null:typeof this.body=="string"?aE:this.body instanceof Fn?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?cE:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,c=n.mode||this.mode,l=n.redirect||this.redirect,u=n.credentials||this.credentials,m=n.referrer||this.referrer,g=n.integrity||this.integrity,_=n.referrerPolicy||this.referrerPolicy,x=n.transferCache??this.transferCache,O=n.timeout??this.timeout,B=n.body!==void 0?n.body:this.body,q=n.withCredentials??this.withCredentials,be=n.reportProgress??this.reportProgress,wt=n.headers||this.headers,gt=n.params||this.params,Nr=n.context??this.context;return n.setHeaders!==void 0&&(wt=Object.keys(n.setHeaders).reduce((cc,Io)=>cc.set(Io,n.setHeaders[Io]),wt)),n.setParams&&(gt=Object.keys(n.setParams).reduce((cc,Io)=>cc.set(Io,n.setParams[Io]),gt)),new t(e,i,B,{params:gt,headers:wt,context:Nr,reportProgress:be,responseType:r,withCredentials:q,transferCache:x,keepalive:o,cache:a,priority:s,timeout:O,mode:c,redirect:l,credentials:u,referrer:m,integrity:g,referrerPolicy:_})}},us=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(us||{}),ba=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new io,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Jm=class t extends ba{constructor(n={}){super(n)}type=us.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},gl=class t extends ba{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=us.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},va=class extends ba{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},hP=200,pP=204;var gP=new y("");var _P=/^\)\]\}',?\n/;var zv=(()=>{class t{xhrFactory;tracingService=d(bi,{optional:!0});constructor(e){this.xhrFactory=e}maybePropagateTrace(e){return this.tracingService?.propagate?this.tracingService.propagate(e):e}handle(e){if(e.method==="JSONP")throw new A(-2800,!1);let i=this.xhrFactory;return Q(null).pipe(_t(()=>new ce(o=>{let s=i.build();if(s.open(e.method,e.urlWithParams),e.withCredentials&&(s.withCredentials=!0),e.headers.forEach((B,q)=>s.setRequestHeader(B,q.join(","))),e.headers.has(sE)||s.setRequestHeader(sE,fP),!e.headers.has(oE)){let B=e.detectContentTypeHeader();B!==null&&s.setRequestHeader(oE,B)}if(e.timeout&&(s.timeout=e.timeout),e.responseType){let B=e.responseType.toLowerCase();s.responseType=B!=="json"?B:"text"}let a=e.serializeBody(),c=null,l=()=>{if(c!==null)return c;let B=s.statusText||"OK",q=new io(s.getAllResponseHeaders()),be=s.responseURL||e.url;return c=new Jm({headers:q,status:s.status,statusText:B,url:be}),c},u=this.maybePropagateTrace(()=>{let{headers:B,status:q,statusText:be,url:wt}=l(),gt=null;q!==pP&&(gt=typeof s.response>"u"?s.responseText:s.response),q===0&&(q=gt?hP:0);let Nr=q>=200&&q<300;if(e.responseType==="json"&&typeof gt=="string"){let cc=gt;gt=gt.replace(_P,"");try{gt=gt!==""?JSON.parse(gt):null}catch(Io){gt=cc,Nr&&(Nr=!1,gt={error:Io,text:gt})}}Nr?(o.next(new gl({body:gt,headers:B,status:q,statusText:be,url:wt||void 0})),o.complete()):o.error(new va({error:gt,headers:B,status:q,statusText:be,url:wt||void 0}))}),m=this.maybePropagateTrace(B=>{let{url:q}=l(),be=new va({error:B,status:s.status||0,statusText:s.statusText||"Unknown Error",url:q||void 0});o.error(be)}),g=m;e.timeout&&(g=this.maybePropagateTrace(B=>{let{url:q}=l(),be=new va({error:new DOMException("Request timed out","TimeoutError"),status:s.status||0,statusText:s.statusText||"Request timeout",url:q||void 0});o.error(be)}));let _=!1,x=this.maybePropagateTrace(B=>{_||(o.next(l()),_=!0);let q={type:us.DownloadProgress,loaded:B.loaded};B.lengthComputable&&(q.total=B.total),e.responseType==="text"&&s.responseText&&(q.partialText=s.responseText),o.next(q)}),O=this.maybePropagateTrace(B=>{let q={type:us.UploadProgress,loaded:B.loaded};B.lengthComputable&&(q.total=B.total),o.next(q)});return s.addEventListener("load",u),s.addEventListener("error",m),s.addEventListener("timeout",g),s.addEventListener("abort",m),e.reportProgress&&(s.addEventListener("progress",x),a!==null&&s.upload&&s.upload.addEventListener("progress",O)),s.send(a),o.next({type:us.Sent}),()=>{s.removeEventListener("error",m),s.removeEventListener("abort",m),s.removeEventListener("load",u),s.removeEventListener("timeout",g),e.reportProgress&&(s.removeEventListener("progress",x),a!==null&&s.upload&&s.upload.removeEventListener("progress",O)),s.readyState!==s.DONE&&s.abort()}})))}static \u0275fac=function(i){return new(i||t)(te(ds))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function vP(t,n){return n(t)}function bP(t,n,e){return(i,r)=>Gt(e,()=>n(i,o=>t(o,r)))}var Uv=new y("",{factory:()=>[]}),lE=new y(""),dE=new y("",{factory:()=>!0});var $v=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=te(zv),r},providedIn:"root"})}return t})();var ef=(()=>{class t{backend;injector;chain=null;pendingTasks=d(Pc);contributeToStability=d(dE);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(Uv),...this.injector.get(lE,[])]));this.chain=i.reduceRight((r,o)=>bP(r,o,this.injector),vP)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(Ni(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(te($v),te(ze))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Gv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=te(ef),r},providedIn:"root"})}return t})();function Hv(t,n){return{body:n,headers:t.headers,context:t.context,observe:t.observe,params:t.params,reportProgress:t.reportProgress,responseType:t.responseType,withCredentials:t.withCredentials,credentials:t.credentials,transferCache:t.transferCache,timeout:t.timeout,keepalive:t.keepalive,priority:t.priority,cache:t.cache,mode:t.mode,redirect:t.redirect,integrity:t.integrity,referrer:t.referrer,referrerPolicy:t.referrerPolicy}}var dn=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof _a)o=e;else{let c;r.headers instanceof io?c=r.headers:c=new io(r.headers);let l;r.params&&(r.params instanceof Fn?l=r.params:l=new Fn({fromObject:r.params})),o=new _a(e,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=Q(o).pipe(Vo(c=>this.handler.handle(c)));if(e instanceof _a||r.observe==="events")return s;let a=s.pipe(xe(c=>c instanceof gl));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(ee(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new A(2806,!1);return c.body}));case"blob":return a.pipe(ee(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new A(2807,!1);return c.body}));case"text":return a.pipe(ee(c=>{if(c.body!==null&&typeof c.body!="string")throw new A(2808,!1);return c.body}));default:return a.pipe(ee(c=>c.body))}case"response":return a;default:throw new A(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new Fn().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,Hv(r,i))}post(e,i,r={}){return this.request("POST",e,Hv(r,i))}put(e,i,r={}){return this.request("PUT",e,Hv(r,i))}static \u0275fac=function(i){return new(i||t)(te(Gv))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var yP=new y("",{factory:()=>!0}),CP="XSRF-TOKEN",xP=new y("",{factory:()=>CP}),wP="X-XSRF-TOKEN",DP=new y("",{factory:()=>wP}),EP=(()=>{class t{cookieName=d(xP);doc=d(K);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=ul(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),uE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=te(EP),r},providedIn:"root"})}return t})();function SP(t,n){if(!d(yP)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=d(pa).href,{origin:o}=new URL(r),{origin:s}=new URL(t.url,o);if(o!==s)return n(t)}catch{return n(t)}let e=d(uE).getToken(),i=d(DP);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}var Wv=(function(t){return t[t.Interceptors=0]="Interceptors",t[t.LegacyInterceptors=1]="LegacyInterceptors",t[t.CustomXsrfConfiguration=2]="CustomXsrfConfiguration",t[t.NoXsrfProtection=3]="NoXsrfProtection",t[t.JsonpSupport=4]="JsonpSupport",t[t.RequestsMadeViaParent=5]="RequestsMadeViaParent",t[t.Fetch=6]="Fetch",t})(Wv||{});function IP(t,n){return{\u0275kind:t,\u0275providers:n}}function qv(...t){let n=[dn,ef,{provide:Gv,useExisting:ef},{provide:$v,useFactory:()=>d(gP,{optional:!0})??d(zv)},{provide:Uv,useValue:SP,multi:!0}];for(let e of t)n.push(...e.\u0275providers);return Bi(n)}function Yv(t){return IP(Wv.Interceptors,t.map(n=>({provide:Uv,useValue:n,multi:!0})))}var mE=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(te(K))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var _l=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=te(MP),r},providedIn:"root"})}return t})(),MP=(()=>{class t extends _l{_doc;constructor(e){super(),this._doc=e}sanitize(e,i){if(i==null)return null;switch(e){case Ht.NONE:return i;case Ht.HTML:return _r(i,"HTML")?vi(i):L_(this._doc,String(i)).toString();case Ht.STYLE:return _r(i,"Style")?vi(i):i;case Ht.SCRIPT:if(_r(i,"Script"))return vi(i);throw new A(5200,!1);case Ht.URL:return _r(i,"URL")?vi(i):Zc(String(i));case Ht.RESOURCE_URL:if(_r(i,"ResourceURL"))return vi(i);throw new A(5201,!1);default:throw new A(5202,!1)}}bypassSecurityTrustHtml(e){return R_(e)}bypassSecurityTrustStyle(e){return O_(e)}bypassSecurityTrustScript(e){return N_(e)}bypassSecurityTrustUrl(e){return P_(e)}bypassSecurityTrustResourceUrl(e){return F_(e)}static \u0275fac=function(i){return new(i||t)(te(K))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var me="primary",Rl=Symbol("RouteTitle"),Jv=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function fs(t){return new Jv(t)}function Zv(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function yE(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let c={},l=t.slice(0,i.length);return Zv(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let a={};return!Zv(o,t.slice(0,o.length),a)||!Zv(s,t.slice(t.length-s.length),a)?null:{consumed:t,posParams:a}}function af(t){return new Promise((n,e)=>{t.pipe(lr()).subscribe({next:i=>n(i),error:i=>e(i)})})}function TP(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!Ki(t[e],n[e]))return!1;return!0}function Ki(t,n){let e=t?eb(t):void 0,i=n?eb(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!CE(t[r],n[r]))return!1;return!0}function eb(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function CE(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function AP(t){return t.length>0?t[t.length-1]:null}function gs(t){return gc(t)?t:Jr(t)?rt(Promise.resolve(t)):Q(t)}function xE(t){return gc(t)?af(t):Promise.resolve(t)}var RP={exact:DE,subset:EE},wE={exact:OP,subset:NP,ignored:()=>!0},hb={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},wl={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function pb(t,n,e){let i=t instanceof bn?t:n.parseUrl(t);return Ct(()=>tb(n.lastSuccessfulNavigation()?.finalUrl??new bn,i,b(b({},wl),e)))}function tb(t,n,e){return RP[e.paths](t.root,n.root,e.matrixParams)&&wE[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function OP(t,n){return Ki(t,n)}function DE(t,n,e){if(!ms(t.segments,n.segments)||!rf(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!DE(t.children[i],n.children[i],e))return!1;return!0}function NP(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>CE(t[e],n[e]))}function EE(t,n,e){return SE(t,n,n.segments,e)}function SE(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!ms(r,e)||n.hasChildren()||!rf(r,e,i))}else if(t.segments.length===e.length){if(!ms(t.segments,e)||!rf(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!EE(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!ms(t.segments,r)||!rf(t.segments,r,i)||!t.children[me]?!1:SE(t.children[me],n,o,i)}}function rf(t,n,e){return n.every((i,r)=>wE[e](t[r].parameters,i.parameters))}var bn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new Pe([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=fs(this.queryParams),this._queryParamMap}toString(){return LP.serialize(this)}},Pe=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return of(this)}},ro=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=fs(this.parameters),this._parameterMap}toString(){return kE(this)}};function PP(t,n){return ms(t,n)&&t.every((e,i)=>Ki(e.parameters,n[i].parameters))}function ms(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function FP(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===me&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==me&&(e=e.concat(n(r,i)))}),e}var ka=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>new oo,providedIn:"root"})}return t})(),oo=class{parse(n){let e=new ib(n);return new bn(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${vl(n.root,!0)}`,i=VP(n.queryParams),r=typeof n.fragment=="string"?`#${BP(n.fragment)}`:"";return`${e}${i}${r}`}},LP=new oo;function of(t){return t.segments.map(n=>kE(n)).join("/")}function vl(t,n){if(!t.hasChildren())return of(t);if(n){let e=t.children[me]?vl(t.children[me],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==me&&i.push(`${r}:${vl(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=FP(t,(i,r)=>r===me?[vl(t.children[me],!1)]:[`${r}:${vl(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[me]!=null?`${of(t)}/${e[0]}`:`${of(t)}/(${e.join("//")})`}}function IE(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function tf(t){return IE(t).replace(/%3B/gi,";")}function BP(t){return encodeURI(t)}function nb(t){return IE(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function sf(t){return decodeURIComponent(t)}function hE(t){return sf(t.replace(/\+/g,"%20"))}function kE(t){return`${nb(t.path)}${jP(t.parameters)}`}function jP(t){return Object.entries(t).map(([n,e])=>`;${nb(n)}=${nb(e)}`).join("")}function VP(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${tf(e)}=${tf(r)}`).join("&"):`${tf(e)}=${tf(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var HP=/^[^\/()?;#]+/;function Qv(t){let n=t.match(HP);return n?n[0]:""}var zP=/^[^\/()?;=#]+/;function UP(t){let n=t.match(zP);return n?n[0]:""}var $P=/^[^=?&#]+/;function GP(t){let n=t.match($P);return n?n[0]:""}var WP=/^[^&#]+/;function qP(t){let n=t.match(WP);return n?n[0]:""}var ib=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new Pe([],{}):new Pe([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new A(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[me]=new Pe(e,i)),r}parseSegment(){let n=Qv(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new A(4009,!1);return this.capture(n),new ro(sf(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=UP(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=Qv(this.remaining);r&&(i=r,this.capture(i))}n[sf(e)]=sf(i)}parseQueryParam(n){let e=GP(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let s=qP(this.remaining);s&&(i=s,this.capture(i))}let r=hE(e),o=hE(i);if(n.hasOwnProperty(r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=Qv(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new A(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=me);let a=this.parseChildren(e+1);i[s??me]=Object.keys(a).length===1&&a[me]?a[me]:new Pe([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new A(4011,!1)}};function ME(t){return t.segments.length>0?new Pe([],{[me]:t}):t}function TE(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=TE(r);if(i===me&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new Pe(t.segments,n);return YP(e)}function YP(t){if(t.numberOfChildren===1&&t.children[me]){let n=t.children[me];return new Pe(t.segments.concat(n.segments),n.children)}return t}function so(t){return t instanceof bn}function AE(t,n,e=null,i=null,r=new oo){let o=RE(t);return OE(o,n,e,i,r)}function RE(t){let n;function e(o){let s={};for(let c of o.children){let l=e(c);s[c.outlet]=l}let a=new Pe(o.url,s);return o===t&&(n=a),a}let i=e(t.root),r=ME(i);return n??r}function OE(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return Xv(o,o,o,e,i,r);let s=ZP(n);if(s.toRoot())return Xv(o,o,new Pe([],{}),e,i,r);let a=QP(s,o,t),c=a.processChildren?yl(a.segmentGroup,a.index,s.commands):PE(a.segmentGroup,a.index,s.commands);return Xv(o,a.segmentGroup,c,e,i,r)}function cf(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function Dl(t){return typeof t=="object"&&t!=null&&t.outlets}function pE(t,n,e){t||="\u0275";let i=new bn;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function Xv(t,n,e,i,r,o){let s={};for(let[l,u]of Object.entries(i??{}))s[l]=Array.isArray(u)?u.map(m=>pE(l,m,o)):pE(l,u,o);let a;t===n?a=e:a=NE(t,n,e);let c=ME(TE(a));return new bn(c,s,r)}function NE(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=NE(o,n,e)}),new Pe(t.segments,i)}var lf=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&cf(i[0]))throw new A(4003,!1);let r=i.find(Dl);if(r&&r!==AP(i))throw new A(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function ZP(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new lf(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?e=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new lf(e,n,i)}var Ca=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function QP(t,n,e){if(t.isAbsolute)return new Ca(n,!0,0);if(!e)return new Ca(n,!1,NaN);if(e.parent===null)return new Ca(e,!0,0);let i=cf(t.commands[0])?0:1,r=e.segments.length-1+i;return XP(e,r,t.numberOfDoubleDots)}function XP(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new A(4005,!1);r=i.segments.length}return new Ca(i,!1,r-o)}function KP(t){return Dl(t[0])?t[0].outlets:{[me]:t}}function PE(t,n,e){if(t??=new Pe([],{}),t.segments.length===0&&t.hasChildren())return yl(t,n,e);let i=JP(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new Pe(t.segments.slice(0,i.pathIndex),{});return o.children[me]=new Pe(t.segments.slice(i.pathIndex),t.children),yl(o,0,r)}else return i.match&&r.length===0?new Pe(t.segments,{}):i.match&&!t.hasChildren()?rb(t,n,e):i.match?yl(t,0,r):rb(t,n,e)}function yl(t,n,e){if(e.length===0)return new Pe(t.segments,{});{let i=KP(e),r={};if(Object.keys(i).some(o=>o!==me)&&t.children[me]&&t.numberOfChildren===1&&t.children[me].segments.length===0){let o=yl(t.children[me],n,e);return new Pe(t.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=PE(t.children[o],n,s))}),Object.entries(t.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new Pe(t.segments,r)}}function JP(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let s=t.segments[r],a=e[i];if(Dl(a))break;let c=`${a}`,l=i<e.length-1?e[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!_E(c,l,s))return o;i+=2}else{if(!_E(c,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function rb(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(Dl(o)){let c=eF(o.outlets);return new Pe(i,c)}if(r===0&&cf(e[0])){let c=t.segments[n];i.push(new ro(c.path,gE(e[0]))),r++;continue}let s=Dl(o)?o.outlets[me]:`${o}`,a=r<e.length-1?e[r+1]:null;s&&a&&cf(a)?(i.push(new ro(s,gE(a))),r+=2):(i.push(new ro(s,{})),r++)}return new Pe(i,{})}function eF(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=rb(new Pe([],{}),0,i))}),n}function gE(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function _E(t,n,e){return t==e.path&&Ki(n,e.parameters)}var Cl="imperative",qt=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(qt||{}),Bn=class{id;url;constructor(n,e){this.id=n,this.url=e}},hs=class extends Bn{type=qt.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},xi=class extends Bn{urlAfterRedirects;type=qt.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},un=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(un||{}),El=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(El||{}),ni=class extends Bn{reason;code;type=qt.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function FE(t){return t instanceof ni&&(t.code===un.Redirect||t.code===un.SupersededByNewNavigation)}var wr=class extends Bn{reason;code;type=qt.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},ps=class extends Bn{error;target;type=qt.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Sl=class extends Bn{urlAfterRedirects;state;type=qt.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},df=class extends Bn{urlAfterRedirects;state;type=qt.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},uf=class extends Bn{urlAfterRedirects;state;shouldActivate;type=qt.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},mf=class extends Bn{urlAfterRedirects;state;type=qt.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},ff=class extends Bn{urlAfterRedirects;state;type=qt.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},hf=class{route;type=qt.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},pf=class{route;type=qt.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},gf=class{snapshot;type=qt.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},_f=class{snapshot;type=qt.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},vf=class{snapshot;type=qt.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},bf=class{snapshot;type=qt.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var wa=class{},Il=class{},Da=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function tF(t){return!(t instanceof wa)&&!(t instanceof Da)&&!(t instanceof Il)}var yf=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new Ma(this.rootInjector)}},Ma=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new yf(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(te(ze))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Cf=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=ob(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=ob(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=sb(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return sb(n,this._root).map(e=>e.value)}};function ob(t,n){if(t===n.value)return n;for(let e of n.children){let i=ob(t,e);if(i)return i}return null}function sb(t,n){if(t===n.value)return[n];for(let e of n.children){let i=sb(t,e);if(i.length)return i.unshift(n),i}return[]}var Ln=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function ya(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var kl=class extends Cf{snapshot;constructor(n,e){super(n),this.snapshot=e,_b(this,n)}toString(){return this.snapshot.toString()}};function LE(t,n){let e=nF(t,n),i=new Bt([new ro("",{})]),r=new Bt({}),o=new Bt({}),s=new Bt({}),a=new Bt(""),c=new yn(i,r,s,a,o,me,t,e.root);return c.snapshot=e.root,new kl(new Ln(c,[]),e)}function nF(t,n){let e={},i={},r={},s=new Ea([],e,r,"",i,me,t,null,{},n);return new Ml("",new Ln(s,[]))}var yn=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(n,e,i,r,o,s,a,c){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(ee(l=>l[Rl]))??Q(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(ee(n=>fs(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(ee(n=>fs(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function gb(t,n,e="emptyOnly"){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:b(b({},n.params),t.params),data:b(b({},n.data),t.data),resolve:b(b(b(b({},t.data),n.data),r?.data),t._resolvedData)}:i={params:b({},t.params),data:b({},t.data),resolve:b(b({},t.data),t._resolvedData??{})},r&&jE(r)&&(i.resolve[Rl]=r.title),i}var Ea=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[Rl]}constructor(n,e,i,r,o,s,a,c,l,u){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=fs(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=fs(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},Ml=class extends Cf{url;constructor(n,e){super(e),this.url=n,_b(this,e)}toString(){return BE(this._root)}};function _b(t,n){n.value._routerState=t,n.children.forEach(e=>_b(t,e))}function BE(t){let n=t.children.length>0?` { ${t.children.map(BE).join(", ")} } `:"";return`${t.value}${n}`}function Kv(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,Ki(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),Ki(n.params,e.params)||t.paramsSubject.next(e.params),TP(n.url,e.url)||t.urlSubject.next(e.url),Ki(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function ab(t,n){let e=Ki(t.params,n.params)&&PP(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||ab(t.parent,n.parent))}function jE(t){return typeof t.title=="string"||t.title===null}var VE=new y(""),Ol=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=me;activateEvents=new H;deactivateEvents=new H;attachEvents=new H;detachEvents=new H;routerOutletData=ha();parentContexts=d(Ma);location=d(Nt);changeDetector=d(ue);inputBinder=d(Ef,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new A(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new A(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new A(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new A(4013,!1);this._activatedRoute=e;let r=this.location,s=e.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new cb(e,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[$e]})}return t})(),cb=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===yn?this.route:n===Ma?this.childContexts:n===VE?this.outletData:this.parent.get(n,e)}},Ef=new y("");var vb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&M(0,"router-outlet")},dependencies:[Ol],encapsulation:2})}return t})();function bb(t){let n=t.children&&t.children.map(bb),e=n?J(b({},t),{children:n}):b({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==me&&(e.component=vb),e}function iF(t,n,e){let i=Tl(t,n._root,e?e._root:void 0);return new kl(i,n)}function Tl(t,n,e){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let i=e.value;i._futureSnapshot=n.value;let r=rF(t,n,e);return new Ln(i,r)}else{if(t.shouldAttach(n.value)){let o=t.retrieve(n.value);if(o!==null){let s=o.route;return s.value._futureSnapshot=n.value,s.children=n.children.map(a=>Tl(t,a)),s}}let i=oF(n.value),r=n.children.map(o=>Tl(t,o));return new Ln(i,r)}}function rF(t,n,e){return n.children.map(i=>{for(let r of e.children)if(t.shouldReuseRoute(i.value,r.value.snapshot))return Tl(t,i,r);return Tl(t,i)})}function oF(t){return new yn(new Bt(t.url),new Bt(t.params),new Bt(t.queryParams),new Bt(t.fragment),new Bt(t.data),t.outlet,t.component,t)}var Sa=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},HE="ngNavigationCancelingError";function xf(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=so(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=zE(!1,un.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function zE(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[HE]=!0,e.cancellationCode=n,e}function sF(t){return UE(t)&&so(t.url)}function UE(t){return!!t&&t[HE]}var lb=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),Kv(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=ya(e);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,e,s.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=ya(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=ya(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(n,e,i){let r=ya(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new bf(o.value.snapshot))}),n.children.length&&this.forwardEvent(new _f(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(Kv(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,s.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),Kv(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},wf=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},xa=class{component;route;constructor(n,e){this.component=n,this.route=e}};function aF(t,n,e){let i=t._root,r=n?n._root:null;return bl(i,r,e,[i.value])}function cF(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function Ta(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!Gp(t)?t:n.get(t):i}function bl(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=ya(n);return t.children.forEach(s=>{lF(s,o[s.value.outlet],e,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>xl(a,e.getContext(s),r)),r}function lF(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,s=n?n.value:null,a=e?e.getContext(t.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let c=dF(s,o,o.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new wf(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?bl(t,n,a?a.children:null,i,r):bl(t,n,e,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new xa(a.outlet.component,s))}else s&&xl(n,a,r),r.canActivateChecks.push(new wf(i)),o.component?bl(t,null,a?a.children:null,i,r):bl(t,null,e,i,r);return r}function dF(t,n,e){if(typeof e=="function")return Gt(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!ms(t.url,n.url);case"pathParamsOrQueryParamsChange":return!ms(t.url,n.url)||!Ki(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!ab(t,n)||!Ki(t.queryParams,n.queryParams);default:return!ab(t,n)}}function xl(t,n,e){let i=ya(t),r=t.value;Object.entries(i).forEach(([o,s])=>{r.component?n?xl(s,n.children.getContext(o),e):xl(s,null,e):xl(s,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new xa(n.outlet.component,r)):e.canDeactivateChecks.push(new xa(null,r)):e.canDeactivateChecks.push(new xa(null,r))}function Nl(t){return typeof t=="function"}function uF(t){return typeof t=="boolean"}function mF(t){return t&&Nl(t.canLoad)}function fF(t){return t&&Nl(t.canActivate)}function hF(t){return t&&Nl(t.canActivateChild)}function pF(t){return t&&Nl(t.canDeactivate)}function gF(t){return t&&Nl(t.canMatch)}function $E(t){return t instanceof ci||t?.name==="EmptyError"}var nf=Symbol("INITIAL_VALUE");function Ia(){return _t(t=>_c(t.map(n=>n.pipe(Ke(1),Je(nf)))).pipe(ee(n=>{for(let e of n)if(e!==!0){if(e===nf)return nf;if(e===!1||_F(e))return e}return!0}),xe(n=>n!==nf),Ke(1)))}function _F(t){return so(t)||t instanceof Sa}function GE(t){return t.aborted?Q(void 0).pipe(Ke(1)):new ce(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function WE(t){return pe(GE(t))}function vF(t){return $t(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?Q(J(b({},n),{guardsResult:!0})):bF(o,e,i).pipe($t(s=>s&&uF(s)?yF(e,r,t):Q(s)),ee(s=>J(b({},n),{guardsResult:s})))})}function bF(t,n,e){return rt(t).pipe($t(i=>EF(i.component,i.route,e,n)),lr(i=>i!==!0,!0))}function yF(t,n,e){return rt(n).pipe(Vo(i=>Oi(xF(i.route.parent,e),CF(i.route,e),DF(t,i.path),wF(t,i.route))),lr(i=>i!==!0,!0))}function CF(t,n){return t!==null&&n&&n(new vf(t)),Q(!0)}function xF(t,n){return t!==null&&n&&n(new gf(t)),Q(!0)}function wF(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return Q(!0);let i=e.map(r=>li(()=>{let o=n._environmentInjector,s=Ta(r,o),a=fF(s)?s.canActivate(n,t):Gt(o,()=>s(n,t));return gs(a).pipe(lr())}));return Q(i).pipe(Ia())}function DF(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>cF(o)).filter(o=>o!==null).map(o=>li(()=>{let s=o.guards.map(a=>{let c=o.node._environmentInjector,l=Ta(a,c),u=hF(l)?l.canActivateChild(e,t):Gt(c,()=>l(e,t));return gs(u).pipe(lr())});return Q(s).pipe(Ia())}));return Q(r).pipe(Ia())}function EF(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return Q(!0);let o=r.map(s=>{let a=n._environmentInjector,c=Ta(s,a),l=pF(c)?c.canDeactivate(t,n,e,i):Gt(a,()=>c(t,n,e,i));return gs(l).pipe(lr())});return Q(o).pipe(Ia())}function SF(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return Q(!0);let s=o.map(a=>{let c=Ta(a,t),l=mF(c)?c.canLoad(n,e):Gt(t,()=>c(n,e)),u=gs(l);return r?u.pipe(WE(r)):u});return Q(s).pipe(Ia(),qE(i))}function qE(t){return Cp(ct(n=>{if(typeof n!="boolean")throw xf(t,n)}),ee(n=>n===!0))}function IF(t,n,e,i,r,o){let s=n.canMatch;if(!s||s.length===0)return Q(!0);let a=s.map(c=>{let l=Ta(c,t),u=gF(l)?l.canMatch(n,e,r):Gt(t,()=>l(n,e,r));return gs(u).pipe(WE(o))});return Q(a).pipe(Ia(),qE(i))}var xr=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},Al=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function kF(t){throw new A(4e3,!1)}function MF(t){throw zE(!1,un.GuardRejected)}var db=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[me])throw kF(`${n.redirectTo}`);r=r.children[me]}}async applyRedirectCommands(n,e,i,r,o){let s=await TF(e,r,o);if(s instanceof bn)throw new Al(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new Al(a);return a}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new bn(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=e[a]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),s={};return Object.entries(e.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(n,c,i,r)}),new Pe(o,s)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new A(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function TF(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return af(gs(Gt(e,()=>i(n))))}function AF(t,n){return t.providers&&!t._injector&&(t._injector=tl(t.providers,n,`Route: ${t.path}`)),t._injector??n}function Ci(t){return t.outlet||me}function RF(t,n){let e=t.filter(i=>Ci(i)===n);return e.push(...t.filter(i=>Ci(i)!==n)),e}var ub={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function YE(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function OF(t,n,e,i,r,o,s){let a=ZE(t,n,e);if(!a.matched)return Q(a);let c=YE(o(a));return i=AF(n,i),IF(i,n,e,r,c,s).pipe(ee(l=>l===!0?a:b({},ub)))}function ZE(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?b({},ub):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||yE)(e,t,n);if(!r)return b({},ub);let o={};Object.entries(r.posParams??{}).forEach(([a,c])=>{o[a]=c.path});let s=r.consumed.length>0?b(b({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function vE(t,n,e,i,r){return e.length>0&&FF(t,e,i,r)?{segmentGroup:new Pe(n,PF(i,new Pe(e,t.children))),slicedSegments:[]}:e.length===0&&LF(t,e,i)?{segmentGroup:new Pe(t.segments,NF(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new Pe(t.segments,t.children),slicedSegments:e}}function NF(t,n,e,i){let r={};for(let o of e)if(Sf(t,n,o)&&!i[Ci(o)]){let s=new Pe([],{});r[Ci(o)]=s}return b(b({},i),r)}function PF(t,n){let e={};e[me]=n;for(let i of t)if(i.path===""&&Ci(i)!==me){let r=new Pe([],{});e[Ci(i)]=r}return e}function FF(t,n,e,i){return e.some(r=>!Sf(t,n,r)||!(Ci(r)!==me)?!1:!(i!==void 0&&Ci(r)===i))}function LF(t,n,e){return e.some(i=>Sf(t,n,i))}function Sf(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function BF(t,n,e){return n.length===0&&!t.children[e]}var mb=class{};async function jF(t,n,e,i,r,o,s="emptyOnly",a){return new fb(t,n,e,i,r,s,o,a).recognize()}var VF=31,fb=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,s,a,c){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new db(this.urlSerializer,this.urlTree)}noMatchError(n){return new A(4002,`'${n.segmentGroup}'`)}async recognize(){let n=vE(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new Ln(i,e),o=new Ml("",r),s=AE(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}}async match(n){let e=new Ea([],Object.freeze({}),Object.freeze(b({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),me,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,me,e),rootSnapshot:e}}catch(i){if(i instanceof Al)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof xr?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let s=await this.processSegment(n,e,i,i.segments,r,!0,o);return s instanceof Ln?[s]:[]}async processChildren(n,e,i,r){let o=[];for(let c of Object.keys(i.children))c==="primary"?o.unshift(c):o.push(c);let s=[];for(let c of o){let l=i.children[c],u=RF(e,c),m=await this.processSegmentGroup(n,u,l,c,r);s.push(...m)}let a=QE(s);return HF(a),a}async processSegment(n,e,i,r,o,s,a){for(let c of e)try{return await this.processSegmentAgainstRoute(c._injector??n,e,c,i,r,o,s,a)}catch(l){if(l instanceof xr||$E(l))continue;throw l}if(BF(i,r,o))return new mb;throw new xr(i)}async processSegmentAgainstRoute(n,e,i,r,o,s,a,c){if(Ci(i)!==s&&(s===me||!Sf(r,o,i)))throw new xr(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,s,c);throw new xr(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,s,a){let{matched:c,parameters:l,consumedSegments:u,positionalParamSegments:m,remainingSegments:g}=ZE(e,r,o);if(!c)throw new xr(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>VF&&(this.allowRedirects=!1));let _=this.createSnapshot(n,r,o,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let x=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,m,YE(_),n),O=await this.applyRedirects.lineralizeSegments(r,x);return this.processSegment(n,i,e,O.concat(g),s,!1,a)}createSnapshot(n,e,i,r,o){let s=new Ea(i,r,Object.freeze(b({},this.urlTree.queryParams)),this.urlTree.fragment,UF(e),Ci(e),e.component??e._loadedComponent??null,e,$F(e),n),a=gb(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(n,e,i,r,o,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=wt=>this.createSnapshot(n,i,wt.consumedSegments,wt.parameters,s),c=await af(OF(e,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(e.children={}),!c?.matched)throw new xr(e);n=i._injector??n;let{routes:l}=await this.getChildConfig(n,i,r),u=i._loadedInjector??n,{parameters:m,consumedSegments:g,remainingSegments:_}=c,x=this.createSnapshot(n,i,g,m,s),{segmentGroup:O,slicedSegments:B}=vE(e,g,_,l,o);if(B.length===0&&O.hasChildren()){let wt=await this.processChildren(u,l,O,x);return new Ln(x,wt)}if(l.length===0&&B.length===0)return new Ln(x,[]);let q=Ci(i)===o,be=await this.processSegment(u,l,O,B,q?me:o,!0,x);return new Ln(x,be instanceof Ln?[be]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await af(SF(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw MF(e)}return{routes:[],injector:n}}};function HF(t){t.sort((n,e)=>n.value.outlet===me?-1:e.value.outlet===me?1:n.value.outlet.localeCompare(e.value.outlet))}function zF(t){let n=t.value.routeConfig;return n&&n.path===""}function QE(t){let n=[],e=new Set;for(let i of t){if(!zF(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=QE(i.children);n.push(new Ln(i.value,r))}return n.filter(i=>!e.has(i))}function UF(t){return t.data||{}}function $F(t){return t.resolve||{}}function GF(t,n,e,i,r,o,s){return $t(async a=>{let{state:c,tree:l}=await jF(t,n,e,i,a.extractedUrl,r,o,s);return J(b({},a),{targetSnapshot:c,urlAfterRedirects:l})})}function WF(t){return $t(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return Q(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let c of XE(a))o.add(c);let s=0;return rt(o).pipe(Vo(a=>r.has(a)?qF(a,e,t):(a.data=gb(a,a.parent,t).resolve,Q(void 0))),ct(()=>s++),su(1),$t(a=>s===o.size?Q(n):Et))})}function XE(t){let n=t.children.map(e=>XE(e)).flat();return[t,...n]}function qF(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!jE(i)&&(r[Rl]=i.title),li(()=>(t.data=gb(t,t.parent,e).resolve,YF(r,t,n).pipe(ee(o=>(t._resolvedData=o,t.data=b(b({},t.data),o),null)))))}function YF(t,n,e){let i=eb(t);if(i.length===0)return Q({});let r={};return rt(i).pipe($t(o=>ZF(t[o],n,e).pipe(lr(),ct(s=>{if(s instanceof Sa)throw xf(new oo,s);r[o]=s}))),su(1),ee(()=>r),di(o=>$E(o)?Et:pc(o)))}function ZF(t,n,e){let i=n._environmentInjector,r=Ta(t,i),o=r.resolve?r.resolve(n,e):Gt(i,()=>r(n,e));return gs(o)}function bE(t){return _t(n=>{let e=t(n);return e?rt(e).pipe(ee(()=>n)):Q(n)})}var yb=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===me);return i}getResolvedTitleForRoute(e){return e.data[Rl]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(KE),providedIn:"root"})}return t})(),KE=(()=>{class t extends yb{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(te(mE))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Aa=new y("",{factory:()=>({})}),Pl=new y(""),JE=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(vv);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await xE(Gt(e,()=>i.loadComponent())),s=await nS(tS(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await eS(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();async function eS(t,n,e,i){let r=await xE(Gt(e,()=>t.loadChildren())),o=await nS(tS(r)),s;o instanceof Dm||Array.isArray(o)?s=o:s=await n.compileModuleAsync(o),i&&i(t);let a,c,l=!1,u;return Array.isArray(s)?(c=s,l=!0):(a=s.create(e).injector,u=s,c=a.get(Pl,[],{optional:!0,self:!0}).flat()),{routes:c.map(bb),injector:a,factory:u}}function QF(t){return t&&typeof t=="object"&&"default"in t}function tS(t){return QF(t)?t.default:t}async function nS(t){return t}var If=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(XF),providedIn:"root"})}return t})(),XF=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),iS=new y("");var KF=()=>{},rS=new y(""),oS=(()=>{class t{currentNavigation=N(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=N(null);events=new I;transitionAbortWithErrorSubject=new I;configLoader=d(JE);environmentInjector=d(ze);destroyRef=d(sn);urlSerializer=d(ka);rootContexts=d(Ma);location=d(Zi);inputBindingEnabled=d(Ef,{optional:!0})!==null;titleStrategy=d(yb);options=d(Aa,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=d(If);createViewTransition=d(iS,{optional:!0});navigationErrorHandler=d(rS,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>Q(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new hf(r)),i=r=>this.events.next(new pf(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;Oe(()=>{this.transitions?.next(J(b({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new Bt(null),this.transitions.pipe(xe(i=>i!==null),_t(i=>{let r=!1,o=new AbortController,s=()=>!r&&this.currentTransition?.id===i.id;return Q(i).pipe(_t(a=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",un.SupersededByNewNavigation),Et;this.currentTransition=i;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:a.id,initialUrl:a.rawUrl,extractedUrl:a.extractedUrl,targetBrowserUrl:typeof a.extras.browserUrl=="string"?this.urlSerializer.parse(a.extras.browserUrl):a.extras.browserUrl,trigger:a.source,extras:a.extras,previousNavigation:c?J(b({},c),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:a.routesRecognizeHandler,beforeActivateHandler:a.beforeActivateHandler});let l=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=a.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!l&&u!=="reload")return this.events.next(new wr(a.id,this.urlSerializer.serialize(a.rawUrl),"",El.IgnoredSameUrlNavigation)),a.resolve(!1),Et;if(this.urlHandlingStrategy.shouldProcessUrl(a.rawUrl))return Q(a).pipe(_t(m=>(this.events.next(new hs(m.id,this.urlSerializer.serialize(m.extractedUrl),m.source,m.restoredState)),m.id!==this.navigationId?Et:Promise.resolve(m))),GF(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),ct(m=>{i.targetSnapshot=m.targetSnapshot,i.urlAfterRedirects=m.urlAfterRedirects,this.currentNavigation.update(g=>(g.finalUrl=m.urlAfterRedirects,g)),this.events.next(new Il)}),_t(m=>rt(i.routesRecognizeHandler.deferredHandle??Q(void 0)).pipe(ee(()=>m))),ct(()=>{let m=new Sl(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(m)}));if(l&&this.urlHandlingStrategy.shouldProcessUrl(a.currentRawUrl)){let{id:m,extractedUrl:g,source:_,restoredState:x,extras:O}=a,B=new hs(m,this.urlSerializer.serialize(g),_,x);this.events.next(B);let q=LE(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=J(b({},a),{targetSnapshot:q,urlAfterRedirects:g,extras:J(b({},O),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(be=>(be.finalUrl=g,be)),Q(i)}else return this.events.next(new wr(a.id,this.urlSerializer.serialize(a.extractedUrl),"",El.IgnoredByUrlHandlingStrategy)),a.resolve(!1),Et}),ee(a=>{let c=new df(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);return this.events.next(c),this.currentTransition=i=J(b({},a),{guards:aF(a.targetSnapshot,a.currentSnapshot,this.rootContexts)}),i}),vF(a=>this.events.next(a)),_t(a=>{if(i.guardsResult=a.guardsResult,a.guardsResult&&typeof a.guardsResult!="boolean")throw xf(this.urlSerializer,a.guardsResult);let c=new uf(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot,!!a.guardsResult);if(this.events.next(c),!s())return Et;if(!a.guardsResult)return this.cancelNavigationTransition(a,"",un.GuardRejected),Et;if(a.guards.canActivateChecks.length===0)return Q(a);let l=new mf(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);if(this.events.next(l),!s())return Et;let u=!1;return Q(a).pipe(WF(this.paramsInheritanceStrategy),ct({next:()=>{u=!0;let m=new ff(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(m)},complete:()=>{u||this.cancelNavigationTransition(a,"",un.NoDataFromResolver)}}))}),bE(a=>{let c=u=>{let m=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let g=u._environmentInjector;m.push(this.configLoader.loadComponent(g,u.routeConfig).then(_=>{u.component=_}))}for(let g of u.children)m.push(...c(g));return m},l=c(a.targetSnapshot.root);return l.length===0?Q(a):rt(Promise.all(l).then(()=>a))}),bE(()=>this.afterPreactivation()),_t(()=>{let{currentSnapshot:a,targetSnapshot:c}=i,l=this.createViewTransition?.(this.environmentInjector,a.root,c.root);return l?rt(l).pipe(ee(()=>i)):Q(i)}),Ke(1),_t(a=>{let c=iF(e.routeReuseStrategy,a.targetSnapshot,a.currentRouterState);this.currentTransition=i=a=J(b({},a),{targetRouterState:c}),this.currentNavigation.update(u=>(u.targetRouterState=c,u)),this.events.next(new wa);let l=i.beforeActivateHandler.deferredHandle;return l?rt(l.then(()=>a)):Q(a)}),ct(a=>{new lb(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),s()&&(r=!0,this.currentNavigation.update(c=>(c.abort=KF,c)),this.lastSuccessfulNavigation.set(Oe(this.currentNavigation)),this.events.next(new xi(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects))),this.titleStrategy?.updateTitle(a.targetRouterState.snapshot),a.resolve(!0))}),pe(GE(o.signal).pipe(xe(()=>!r&&!i.targetRouterState),ct(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",un.Aborted)}))),ct({complete:()=>{r=!0}}),pe(this.transitionAbortWithErrorSubject.pipe(ct(a=>{throw a}))),Ni(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",un.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),di(a=>{if(r=!0,this.destroyed)return i.resolve(!1),Et;if(UE(a))this.events.next(new ni(i.id,this.urlSerializer.serialize(i.extractedUrl),a.message,a.cancellationCode)),sF(a)?this.events.next(new Da(a.url,a.navigationBehaviorOptions)):i.resolve(!1);else{let c=new ps(i.id,this.urlSerializer.serialize(i.extractedUrl),a,i.targetSnapshot??void 0);try{let l=Gt(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(l instanceof Sa){let{message:u,cancellationCode:m}=xf(this.urlSerializer,l);this.events.next(new ni(i.id,this.urlSerializer.serialize(i.extractedUrl),u,m)),this.events.next(new Da(l.redirectTo,l.navigationBehaviorOptions))}else throw this.events.next(c),a}catch(l){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(l)}}return Et}))}))}cancelNavigationTransition(e,i,r){let o=new ni(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Oe(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function JF(t){return t!==Cl}var sS=new y("");var aS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(eL),providedIn:"root"})}return t})(),Df=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},eL=(()=>{class t extends Df{static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),kf=(()=>{class t{urlSerializer=d(ka);options=d(Aa,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(Zi);urlHandlingStrategy=d(If);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new bn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,s=r??o;return s instanceof bn?this.urlSerializer.serialize(s):s}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=LE(null,d(ze));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(tL),providedIn:"root"})}return t})(),tL=(()=>{class t extends kf{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof hs?this.updateStateMemento():e instanceof wr?this.commitTransition(i):e instanceof Sl?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof wa?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof ni&&!FE(e)?this.restoreHistory(i):e instanceof ps?this.restoreHistory(i,!0):e instanceof xi&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(e)||s){let c=this.browserPageId,l=b(b({},a),this.generateNgRouterState(o,c,i));this.location.replaceState(e,"",l)}else{let c=b(b({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",c)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?b({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):b({navigationId:e},this.routerUrlState(r))}static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Cb(t,n){t.events.pipe(xe(e=>e instanceof xi||e instanceof ni||e instanceof ps||e instanceof wr),ee(e=>e instanceof xi||e instanceof wr?0:(e instanceof ni?e.code===un.Redirect||e.code===un.SupersededByNewNavigation:!1)?2:1),xe(e=>e!==2),Ke(1)).subscribe(()=>{n()})}var Mt=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d(Em);stateManager=d(kf);options=d(Aa,{optional:!0})||{};pendingTasks=d(pr);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(oS);urlSerializer=d(ka);location=d(Zi);urlHandlingStrategy=d(If);injector=d(ze);_events=new I;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d(aS);injectorCleanup=d(sS,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(Pl,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(Ef,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new ve;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=Oe(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof ni&&i.code!==un.Redirect&&i.code!==un.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof xi)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof Da){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=b({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||JF(r.source)},s);this.scheduleNavigation(a,Cl,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}tF(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Cl,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let s=r?.navigationId?r:null,a=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=J(b({},o),{browserUrl:e})),r){let l=b({},r);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(o.state=l)}let c=this.parseUrl(a);this.scheduleNavigation(c,i,s,o).catch(l=>{this.disposed||this.injector.get(Pn)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Oe(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(bb),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:s,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=b(b({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let m;try{let g=r?r.snapshot:this.routerState.snapshot.root;m=RE(g)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),m=this.currentUrlTree.root}return OE(m,e,u,l??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=so(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Cl,null,i)}navigate(e,i={skipLocationChange:!1}){return nL(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(Li(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=b({},hb):i===!1?r=b({},wl):r=b(b({},wl),i),so(e))return tb(this.currentUrlTree,e,r);let o=this.parseUrl(e);return tb(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,c,l;s?(a=s.resolve,c=s.reject,l=s.promise):l=new Promise((m,g)=>{a=m,c=g});let u=this.pendingTasks.add();return Cb(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function nL(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new A(4008,!1)}var rL=(()=>{class t{router=d(Mt);stateManager=d(kf);fragment=N("");queryParams=N({});path=N("");serializer=d(ka);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof xi&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new bn(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),nt=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=d(new Jn("href"),{optional:!0});reactiveHref=yv(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return Oe(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return Oe(this._target)}_target=N(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return Oe(this._queryParams)}_queryParams=N(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return Oe(this._fragment)}_fragment=N(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return Oe(this._queryParamsHandling)}_queryParamsHandling=N(void 0);set state(e){this._state.set(e)}get state(){return Oe(this._state)}_state=N(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return Oe(this._info)}_info=N(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return Oe(this._relativeTo)}_relativeTo=N(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return Oe(this._preserveFragment)}_preserveFragment=N(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return Oe(this._skipLocationChange)}_skipLocationChange=N(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return Oe(this._replaceUrl)}_replaceUrl=N(!1);isAnchorElement;onChanges=new I;applicationErrorHandler=d(Pn);options=d(Aa,{optional:!0});reactiveRouterState=d(rL);constructor(e,i,r,o,s,a){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=s,this.locationStrategy=a;let c=s.nativeElement.tagName?.toLowerCase();this.isAnchorElement=c==="a"||c==="area"||!!(typeof customElements=="object"&&customElements.get(c)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=N(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(so(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,s){let a=this._urlTree();if(a===null||this.isAnchorElement&&(e!==0||i||r||o||s||typeof this.target=="string"&&this.target!="_self"))return!0;let c={skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info};return this.router.navigateByUrl(a,c)?.catch(l=>{this.applicationErrorHandler(l)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=Ct(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:so(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return Oe(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(ie(Mt),ie(yn),qc("tabindex"),ie(we),ie(P),ie(ga))};static \u0275dir=T({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&R("click",function(s){return r.onClick(s.button,s.ctrlKey,s.shiftKey,s.altKey,s.metaKey)}),i&2&&Y("href",r.reactiveHref(),B_)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",V],skipLocationChange:[2,"skipLocationChange","skipLocationChange",V],replaceUrl:[2,"replaceUrl","replaceUrl",V],routerLink:"routerLink"},features:[$e]})}return t})(),xb=(()=>{class t{router;element;renderer;cdr;links;classes=[];routerEventsSubscription;linkInputChangesSubscription;_isActive=!1;get isActive(){return this._isActive}routerLinkActiveOptions={exact:!1};ariaCurrentWhenActive;isActiveChange=new H;link=d(nt,{optional:!0});constructor(e,i,r,o){this.router=e,this.element=i,this.renderer=r,this.cdr=o,this.routerEventsSubscription=e.events.subscribe(s=>{s instanceof xi&&this.update()})}ngAfterContentInit(){Q(this.links.changes,Q(null)).pipe(jr()).subscribe(e=>{this.update(),this.subscribeToEachLinkOnChanges()})}subscribeToEachLinkOnChanges(){this.linkInputChangesSubscription?.unsubscribe();let e=[...this.links.toArray(),this.link].filter(i=>!!i).map(i=>i.onChanges);this.linkInputChangesSubscription=rt(e).pipe(jr()).subscribe(i=>{this._isActive!==this.isLinkActive(this.router)(i)&&this.update()})}set routerLinkActive(e){let i=Array.isArray(e)?e:e.split(" ");this.classes=i.filter(r=>!!r)}ngOnChanges(e){this.update()}ngOnDestroy(){this.routerEventsSubscription.unsubscribe(),this.linkInputChangesSubscription?.unsubscribe()}update(){!this.links||!this.router.navigated||queueMicrotask(()=>{let e=this.hasActiveLinks();this.classes.forEach(i=>{e?this.renderer.addClass(this.element.nativeElement,i):this.renderer.removeClass(this.element.nativeElement,i)}),e&&this.ariaCurrentWhenActive!==void 0?this.renderer.setAttribute(this.element.nativeElement,"aria-current",this.ariaCurrentWhenActive.toString()):this.renderer.removeAttribute(this.element.nativeElement,"aria-current"),this._isActive!==e&&(this._isActive=e,this.cdr.markForCheck(),this.isActiveChange.emit(e))})}isLinkActive(e){let i=oL(this.routerLinkActiveOptions)?this.routerLinkActiveOptions:this.routerLinkActiveOptions.exact??!1?b({},hb):b({},wl);return r=>{let o=r.urlTree;return o?Oe(pb(o,e,i)):!1}}hasActiveLinks(){let e=this.isLinkActive(this.router);return this.link&&e(this.link)||this.links.some(e)}static \u0275fac=function(i){return new(i||t)(ie(Mt),ie(P),ie(we),ie(ue))};static \u0275dir=T({type:t,selectors:[["","routerLinkActive",""]],contentQueries:function(i,r,o){if(i&1&&yt(o,nt,5),i&2){let s;U(s=$())&&(r.links=s)}},inputs:{routerLinkActiveOptions:"routerLinkActiveOptions",ariaCurrentWhenActive:"ariaCurrentWhenActive",routerLinkActive:"routerLinkActive"},outputs:{isActiveChange:"isActiveChange"},exportAs:["routerLinkActive"],features:[$e]})}return t})();function oL(t){let n=t;return!!(n.paths||n.matrixParams||n.queryParams||n.fragment)}var sL=new y("");function wb(t,...n){return Bi([{provide:Pl,multi:!0,useValue:t},[],{provide:yn,useFactory:aL},{provide:Im,multi:!0,useFactory:cL},n.map(e=>e.\u0275providers)])}function aL(){return d(Mt).routerState.root}function cL(){let t=d(X);return n=>{let e=t.get(En);if(n!==e.components[0])return;let i=t.get(Mt),r=t.get(lL);t.get(dL)===1&&i.initialNavigation(),t.get(uL,null,{optional:!0})?.setUpPreloading(),t.get(sL,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var lL=new y("",{factory:()=>new I}),dL=new y("",{factory:()=>1});var uL=new y("");var Mf=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-home"]],decls:8,vars:0,consts:[[1,"max-w-screen-2xl","mx-auto","px-4","mt-32"],[1,"flex","flex-col","items-center","py-16","justify-center","mt-20","rounded-2xl","shadow-xl","relative"],["src","/ProductImages/banner.png","alt","",1,"absolute","insert-0","w-full","h-full","object-cover","rounded-2xl"],[1,"flex","flex-col","p-8","rounded-2xl","items-center","relative"],[1,"-translate-y-25","my-6","font-extrabold","text-black","text-6xl"],["routerLink","/shop",1,"shopButton","cursor-pointer","bg-linear-to-r","from-purple-600","to-violet-400","font-medium","text-2xl","text-black","rounded-2xl","px-8","py-4","border-4","border-black","mt-8"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"div",1),M(2,"img",2),f(3,"div",3)(4,"h1",4),v(5," Welcome To TechTronica "),h(),f(6,"button",5),v(7," Go To Shop "),h()()()())},dependencies:[nt],styles:[".shopButton[_ngcontent-%COMP%]:hover{transform:scale(1.05);box-shadow:0 20px 40px #0003}"]})};var ao=class{brands=[];category=[];sort="name";pageNumber=1;pageSize=10;search=""};var Kt={production:!0,apiUrl:"api/",hubUrl:"hub/notifications",stripePublicKey:"pk_test_51TTYK5BXcBkBxl1bAFHzP2EHxzu4SjxIR7cSuYTGiHh2tsWAYIEZUmd1zDqFVyvSDIsQEZN4Yri4vDcni6WXGCVW00U78NRwZs"};var Ji=class t{http=d(dn);baseUrl=Kt.apiUrl;category=[];brands=[];searchChanged=N("");shopParams=new ao;getProducts(n){let e=new Fn;return n.brands.length>0&&(e=e.append("brand",n.brands.join(","))),n.category&&n.category.length>0&&(e=e.append("category",n.category.join(","))),n.sort&&(e=e.append("sort",n.sort)),n.search&&(e=e.append("search",n.search)),e=e.append("pageSize",n.pageSize),e=e.append("pageNum",n.pageNumber),console.log("params:",e.toString()),this.http.get(this.baseUrl+"products",{params:e})}getProduct(n){return this.http.get(this.baseUrl+"products/"+n)}getBrands(){if(!(this.brands.length>0))return this.http.get(this.baseUrl+"products/brands").subscribe({next:n=>this.brands=n})}getCategory(){if(!(this.category.length>0))return this.http.get(this.baseUrl+"products/category").subscribe({next:n=>this.category=n})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var mL=new y("cdk-dir-doc",{providedIn:"root",factory:()=>d(K)}),fL=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function cS(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?fL.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Jt=(()=>{class t{get value(){return this.valueSignal()}valueSignal=N("ltr");change=new H;constructor(){let e=d(mL,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(cS(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ye=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({})}return t})();var hL=["*"];var pL=new y("MAT_CARD_CONFIG"),co=(()=>{class t{appearance;constructor(){let e=d(pL,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&z("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:hL,decls:1,vars:0,template:function(i,r){i&1&&(Se(),se(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2,changeDetection:0})}return t})();var lS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})();var dS=(()=>{class t{align="start";static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["mat-card-actions"]],hostAttrs:[1,"mat-mdc-card-actions","mdc-card__actions"],hostVars:2,hostBindings:function(i,r){i&2&&z("mat-mdc-card-actions-align-end",r.align==="end")},inputs:{align:"align"},exportAs:["matCardActions"]})}return t})();var uS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[ye]})}return t})();function _s(t){return t.buttons===0||t.detail===0}function vs(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var Db;function mS(){if(Db==null){let t=typeof document<"u"?document.head:null;Db=!!(t&&(t.createShadowRoot||t.attachShadow))}return Db}function Eb(t){if(mS()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Dr(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function en(t){return t.composedPath?t.composedPath()[0]:t.target}var Sb;try{Sb=typeof Intl<"u"&&Intl.v8BreakIterator}catch{Sb=!1}var Ce=(()=>{class t{_platformId=d(ss);isBrowser=this._platformId?WD(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Sb)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Fl;function fS(){if(Fl==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Fl=!0}))}finally{Fl=Fl||!1}return Fl}function Ra(t){return fS()?t:!!t.capture}function jn(t,n=0){return hS(t)?Number(t):arguments.length===2?n:0}function hS(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function Cn(t){return t instanceof P?t.nativeElement:t}var pS=new y("cdk-input-modality-detector-options"),gS={ignoreKeys:[18,17,224,91,16]},_S=650,Ib={passive:!0,capture:!0},vS=(()=>{class t{_platform=d(Ce);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Bt(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=en(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<_S||(this._modality.next(_s(e)?"keyboard":"mouse"),this._mostRecentTarget=en(e))};_onTouchstart=e=>{if(vs(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=en(e)};constructor(){let e=d(j),i=d(K),r=d(pS,{optional:!0});if(this._options=b(b({},gS),r),this.modalityDetected=this._modality.pipe(bc(1)),this.modalityChanged=this.modalityDetected.pipe(ou()),this._platform.isBrowser){let o=d(Ot).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Ib),o.listen(i,"mousedown",this._onMousedown,Ib),o.listen(i,"touchstart",this._onTouchstart,Ib)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ll=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(Ll||{}),bS=new y("cdk-focus-monitor-default-options"),Tf=Ra({passive:!0,capture:!0}),Mn=(()=>{class t{_ngZone=d(j);_platform=d(Ce);_inputModalityDetector=d(vS);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(K);_stopInputModalityDetector=new I;constructor(){let e=d(bS,{optional:!0});this._detectionMode=e?.detectionMode||Ll.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=en(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=Cn(e);if(!this._platform.isBrowser||r.nodeType!==1)return Q();let o=Eb(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new I,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=Cn(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=Cn(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Ll.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===Ll.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?_S:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=en(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Tf),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Tf)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(pe(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Tf),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Tf),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Af=new WeakMap,qe=(()=>{class t{_appRef;_injector=d(X);_environmentInjector=d(ze);load(e){let i=this._appRef=this._appRef||this._injector.get(En),r=Af.get(i);r||(r={loaders:new Set,refs:[]},Af.set(i,r),i.onDestroy(()=>{Af.get(i)?.refs.forEach(o=>o.destroy()),Af.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Pm(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var er=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2,changeDetection:0})}return t})(),Rf;function _L(){if(Rf===void 0&&(Rf=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(Rf=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return Rf}function bs(t){return _L()?.createHTML(t)||t}function yS(t,n,e){let i=e.sanitize(Ht.HTML,n);t.innerHTML=bs(i||"")}function Oa(t){return Array.isArray(t)?t:[t]}var CS=new Set,ys,Na=(()=>{class t{_platform=d(Ce);_nonce=d(as,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):bL}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&vL(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function vL(t,n){if(!CS.has(t))try{ys||(ys=document.createElement("style"),n&&ys.setAttribute("nonce",n),ys.setAttribute("type","text/css"),document.head.appendChild(ys)),ys.sheet&&(ys.sheet.insertRule(`@media ${t} {body{ }}`,0),CS.add(t))}catch(e){console.error(e)}}function bL(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var Bl=(()=>{class t{_mediaMatcher=d(Na);_zone=d(j);_queries=new Map;_destroySubject=new I;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return xS(Oa(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=xS(Oa(e)).map(s=>this._registerQuery(s).observable),o=_c(r);return o=Oi(o.pipe(Ke(1)),o.pipe(bc(1),Ho(0))),o.pipe(ee(s=>{let a={matches:!1,breakpoints:{}};return s.forEach(({matches:c,query:l})=>{a.matches=a.matches||c,a.breakpoints[l]=c}),a}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new ce(s=>{let a=c=>this._zone.run(()=>s.next(c));return i.addListener(a),()=>{i.removeListener(a)}}).pipe(Je(i),ee(({matches:s})=>({query:e,matches:s})),pe(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function xS(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}function yL(t){if(t.type==="characterData"&&t.target instanceof Comment)return!0;if(t.type==="childList"){for(let n=0;n<t.addedNodes.length;n++)if(!(t.addedNodes[n]instanceof Comment))return!1;for(let n=0;n<t.removedNodes.length;n++)if(!(t.removedNodes[n]instanceof Comment))return!1;return!0}return!1}var wS=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),DS=(()=>{class t{_mutationObserverFactory=d(wS);_observedElements=new Map;_ngZone=d(j);constructor(){}ngOnDestroy(){this._observedElements.forEach((e,i)=>this._cleanupObserver(i))}observe(e){let i=Cn(e);return new ce(r=>{let s=this._observeElement(i).pipe(ee(a=>a.filter(c=>!yL(c))),xe(a=>!!a.length)).subscribe(a=>{this._ngZone.run(()=>{r.next(a)})});return()=>{s.unsubscribe(),this._unobserveElement(i)}})}_observeElement(e){return this._ngZone.runOutsideAngular(()=>{if(this._observedElements.has(e))this._observedElements.get(e).count++;else{let i=new I,r=this._mutationObserverFactory.create(o=>i.next(o));r&&r.observe(e,{characterData:!0,childList:!0,subtree:!0}),this._observedElements.set(e,{observer:r,stream:i,count:1})}return this._observedElements.get(e).stream})}_unobserveElement(e){this._observedElements.has(e)&&(this._observedElements.get(e).count--,this._observedElements.get(e).count||this._cleanupObserver(e))}_cleanupObserver(e){if(this._observedElements.has(e)){let{observer:i,stream:r}=this._observedElements.get(e);i&&i.disconnect(),r.complete(),this._observedElements.delete(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ES=(()=>{class t{_contentObserver=d(DS);_elementRef=d(P);event=new H;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._disabled?this._unsubscribe():this._subscribe()}_disabled=!1;get debounce(){return this._debounce}set debounce(e){this._debounce=jn(e),this._subscribe()}_debounce;_currentSubscription=null;constructor(){}ngAfterContentInit(){!this._currentSubscription&&!this.disabled&&this._subscribe()}ngOnDestroy(){this._unsubscribe()}_subscribe(){this._unsubscribe();let e=this._contentObserver.observe(this._elementRef);this._currentSubscription=(this.debounce?e.pipe(Ho(this.debounce)):e).subscribe(this.event)}_unsubscribe(){this._currentSubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","cdkObserveContent",""]],inputs:{disabled:[2,"cdkObserveContentDisabled","disabled",V],debounce:"debounce"},outputs:{event:"cdkObserveContent"},exportAs:["cdkObserveContent"]})}return t})(),jl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({providers:[wS]})}return t})();var Pa=(()=>{class t{_platform=d(Ce);constructor(){}isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return xL(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=CL(TL(e));if(i&&(SS(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=SS(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!kL(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return ML(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function CL(t){try{return t.frameElement}catch{return null}}function xL(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function wL(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function DL(t){return SL(t)&&t.type=="hidden"}function EL(t){return IL(t)&&t.hasAttribute("href")}function SL(t){return t.nodeName.toLowerCase()=="input"}function IL(t){return t.nodeName.toLowerCase()=="a"}function MS(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function SS(t){if(!MS(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function kL(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function ML(t){return DL(t)?!1:wL(t)||EL(t)||t.hasAttribute("contenteditable")||MS(t)}function TL(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var Of=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,s){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=s,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){this._injector?bt(n,{injector:this._injector}):setTimeout(n)}},Mb=(()=>{class t{_checker=d(Pa);_ngZone=d(j);_document=d(K);_injector=d(X);constructor(){d(qe).load(er)}create(e,i=!1){return new Of(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var TS=new y("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),AS=new y("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),AL=0,Vl=(()=>{class t{_ngZone=d(j);_defaultOptions=d(AS,{optional:!0});_liveElement;_document=d(K);_sanitizer=d(_l);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(TS,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:yS(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${AL++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var lo=(function(t){return t[t.NONE=0]="NONE",t[t.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",t[t.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",t})(lo||{}),IS="cdk-high-contrast-black-on-white",kS="cdk-high-contrast-white-on-black",kb="cdk-high-contrast-active",RS=(()=>{class t{_platform=d(Ce);_hasCheckedHighContrastMode=!1;_document=d(K);_breakpointSubscription;constructor(){this._breakpointSubscription=d(Bl).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return lo.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let i=this._document.defaultView||window,r=i&&i.getComputedStyle?i.getComputedStyle(e):null,o=(r&&r.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return lo.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return lo.BLACK_ON_WHITE}return lo.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(kb,IS,kS),this._hasCheckedHighContrastMode=!0;let i=this.getHighContrastMode();i===lo.BLACK_ON_WHITE?e.add(kb,IS):i===lo.WHITE_ON_BLACK&&e.add(kb,kS)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Hl=(()=>{class t{constructor(){d(RS)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[jl]})}return t})();var RL=200,Nf=class{_letterKeyStream=new I;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new I;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:RL;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(ct(e=>this._pressedLetters.push(e)),Ho(n),xe(()=>this._pressedLetters.length>0),ee(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function xt(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var Fa=class{_items;_activeItemIndex=N(-1);_activeItem=N(null);_wrap=!1;_typeaheadSubscription=ve.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof Dn?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Yi(n)&&(this._effectRef=hi(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new I;change=new I;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new Nf(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||xt(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Yi(this._items)?this._items():this._items instanceof Dn?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Wl=class extends Fa{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var Er=class extends Fa{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var Tb={},Ie=class t{_appId=d(Qr);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){return this._appId!=="ng"&&(n+=this._appId),Tb.hasOwnProperty(n)||(Tb[n]=0),`${n}${e?t._infix+"-":""}${Tb[n]++}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var NS=" ";function Ob(t,n,e){let i=Lf(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(NS)))}function Bf(t,n,e){let i=Lf(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(NS)):t.removeAttribute(n)}function Lf(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var PS="cdk-describedby-message",Ff="cdk-describedby-host",Rb=0,jf=(()=>{class t{_platform=d(Ce);_document=d(K);_messageRegistry=new Map;_messagesContainer=null;_id=`${Rb++}`;constructor(){d(qe).load(er),this._id=d(Qr)+"-"+Rb++}describe(e,i,r){if(!this._canBeDescribed(e,i))return;let o=Ab(i,r);typeof i!="string"?(OS(i,this._id),this._messageRegistry.set(o,{messageElement:i,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(i,r),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,i,r){if(!i||!this._isElementNode(e))return;let o=Ab(i,r);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof i=="string"){let s=this._messageRegistry.get(o);s&&s.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${Ff}="${this._id}"]`);for(let i=0;i<e.length;i++)this._removeCdkDescribedByReferenceIds(e[i]),e[i].removeAttribute(Ff);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,i){let r=this._document.createElement("div");OS(r,this._id),r.textContent=e,i&&r.setAttribute("role",i),this._createMessagesContainer(),this._messagesContainer.appendChild(r),this._messageRegistry.set(Ab(e,i),{messageElement:r,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",i=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<i.length;o++)i[o].remove();let r=this._document.createElement("div");r.style.visibility="hidden",r.classList.add(e),r.classList.add("cdk-visually-hidden"),this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._messagesContainer=r}_removeCdkDescribedByReferenceIds(e){let i=Lf(e,"aria-describedby").filter(r=>r.indexOf(PS)!=0);e.setAttribute("aria-describedby",i.join(" "))}_addMessageReference(e,i){let r=this._messageRegistry.get(i);Ob(e,"aria-describedby",r.messageElement.id),e.setAttribute(Ff,this._id),r.referenceCount++}_removeMessageReference(e,i){let r=this._messageRegistry.get(i);r.referenceCount--,Bf(e,"aria-describedby",r.messageElement.id),e.removeAttribute(Ff)}_isElementDescribedByMessage(e,i){let r=Lf(e,"aria-describedby"),o=this._messageRegistry.get(i),s=o&&o.messageElement.id;return!!s&&r.indexOf(s)!=-1}_canBeDescribed(e,i){if(!this._isElementNode(e))return!1;if(i&&typeof i=="object")return!0;let r=i==null?"":`${i}`.trim(),o=e.getAttribute("aria-label");return r?!o||o.trim()!==r:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Ab(t,n){return typeof t=="string"?`${n||""}/${t}`:t}function OS(t,n){t.id||(t.id=`${PS}-${n}-${Rb++}`)}var Cs;function FS(){if(Cs==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return Cs=!1,Cs;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)Cs=!0;else{let t=Element.prototype.scrollTo;t?Cs=!/\{\s*\[native code\]\s*\}/.test(t.toString()):Cs=!1}}return Cs}function Nb(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var La,LS=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Pb(){if(La)return La;if(typeof document!="object"||!document)return La=new Set(LS),La;let t=document.createElement("input");return La=new Set(LS.filter(n=>(t.setAttribute("type",n),t.type===n))),La}var BS={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var OL=new y("MATERIAL_ANIMATIONS"),jS=null;function ql(){return d(OL,{optional:!0})?.animationsDisabled||d(Yc,{optional:!0})==="NoopAnimations"?"di-disabled":(jS??=d(Na).matchMedia("(prefers-reduced-motion)").matches,jS?"reduced-motion":"enabled")}function ke(){return ql()!=="enabled"}function Tt(t){return t==null?"":typeof t=="string"?t:`${t}px`}function At(t){return t!=null&&`${t}`!="false"}var ii=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(ii||{}),Fb=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=ii.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},VS=Ra({passive:!0,capture:!0}),Lb=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,VS)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,VS)))}_delegateEventHandler=n=>{let e=en(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},Yl={enterDuration:225,exitDuration:150},NL=800,HS=Ra({passive:!0,capture:!0}),zS=["mousedown","touchstart"],US=["mouseup","mouseleave","touchend","touchcancel"],PL=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return t})(),xs=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Lb;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=Cn(i)),o&&o.get(qe).load(PL)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=b(b({},Yl),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||FL(n,e,r),a=n-r.left,c=e-r.top,l=o.enterDuration,u=document.createElement("div");u.classList.add("mat-ripple-element"),u.style.left=`${a-s}px`,u.style.top=`${c-s}px`,u.style.height=`${s*2}px`,u.style.width=`${s*2}px`,i.color!=null&&(u.style.backgroundColor=i.color),u.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(u);let m=window.getComputedStyle(u),g=m.transitionProperty,_=m.transitionDuration,x=g==="none"||_==="0s"||_==="0s, 0s"||r.width===0&&r.height===0,O=new Fb(this,u,i,x);u.style.transform="scale3d(1, 1, 1)",O.state=ii.FADING_IN,i.persistent||(this._mostRecentTransientRipple=O);let B=null;return!x&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let q=()=>{B&&(B.fallbackTimer=null),clearTimeout(wt),this._finishRippleTransition(O)},be=()=>this._destroyRipple(O),wt=setTimeout(be,l+100);u.addEventListener("transitionend",q),u.addEventListener("transitioncancel",be),B={onTransitionEnd:q,onTransitionCancel:be,fallbackTimer:wt}}),this._activeRipples.set(O,B),(x||!l)&&this._finishRippleTransition(O),O}fadeOutRipple(n){if(n.state===ii.FADING_OUT||n.state===ii.HIDDEN)return;let e=n.element,i=b(b({},Yl),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=ii.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=Cn(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,zS.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{US.forEach(e=>{this._triggerElement.addEventListener(e,this,HS)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===ii.FADING_IN?this._startFadeOutTransition(n):n.state===ii.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=ii.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=ii.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=_s(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+NL;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!vs(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===ii.VISIBLE||n.config.terminateOnPointerUp&&n.state===ii.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(zS.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(US.forEach(e=>n.removeEventListener(e,this,HS)),this._pointerUpEventsRegistered=!1))}};function FL(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var Zl=new y("mat-ripple-global-options"),tr=(()=>{class t{_elementRef=d(P);_animationsDisabled=ke();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(j),i=d(Ce),r=d(Zl,{optional:!0}),o=d(X);this._globalOptions=r||{},this._rippleRenderer=new xs(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:b(b(b({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,b(b({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,b(b({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&z("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var LL={capture:!0},BL=["focus","mousedown","mouseenter","touchstart"],Bb="mat-ripple-loader-uninitialized",jb="mat-ripple-loader-class-name",$S="mat-ripple-loader-centered",Vf="mat-ripple-loader-disabled",GS=(()=>{class t{_document=d(K);_animationsDisabled=ke();_globalRippleOptions=d(Zl,{optional:!0});_platform=d(Ce);_ngZone=d(j);_injector=d(X);_eventCleanups;_hosts=new Map;constructor(){let e=d(Ot).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>BL.map(i=>e.listen(this._document,i,this._onInteraction,LL)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(Bb,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(jb))&&e.setAttribute(jb,i.className||""),i.centered&&e.setAttribute($S,""),i.disabled&&e.setAttribute(Vf,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(Vf,""):e.removeAttribute(Vf)}_onInteraction=e=>{let i=en(e);if(i instanceof HTMLElement){let r=i.closest(`[${Bb}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(jb)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Yl.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??Yl.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(Vf),rippleConfig:{centered:e.hasAttribute($S),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new xs(a,this._ngZone,i,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:c,hasSetUpEvents:l}),e.removeAttribute(Bb)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Tn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var jL=["mat-icon-button",""],VL=["*"],HL=new y("MAT_BUTTON_CONFIG");function WS(t){return t==null?void 0:ut(t)}var Vb=(()=>{class t{_elementRef=d(P);_ngZone=d(j);_animationsDisabled=ke();_config=d(HL,{optional:!0});_focusMonitor=d(Mn);_cleanupClick;_renderer=d(we);_rippleLoader=d(GS);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}constructor(){d(qe).load(Tn);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(Y("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),It(r.color?"mat-"+r.color:""),z("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",V],disabled:[2,"disabled","disabled",V],ariaDisabled:[2,"aria-disabled","ariaDisabled",V],disabledInteractive:[2,"disabledInteractive","disabledInteractive",V],tabIndex:[2,"tabIndex","tabIndex",WS],_tabindex:[2,"tabindex","_tabindex",WS]}})}return t})(),ws=(()=>{class t extends Vb{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[_e],attrs:jL,ngContentSelectors:VL,decls:4,vars:0,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Se(),zt(0,"span",0),se(1),zt(2,"span",1)(3,"span",2))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--mat-icon-button-state-layer-size, 40px);
  height: var(--mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--mat-icon-button-state-layer-size, 40px) - var(--mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--mat-icon-button-icon-size, 24px);
  color: var(--mat-icon-button-icon-color, var(--mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--mat-icon-button-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface-variant) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-icon-button-touch-target-size, 48px);
  display: var(--mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--mat-icon-button-icon-size, 24px);
  height: var(--mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var uo=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[ye]})}return t})();var zL=["matButton",""],UL=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],$L=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var qS=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),pt=(()=>{class t extends Vb{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=GL(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?qS.get(this._appearance):null,o=qS.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[_e],attrs:zL,ngContentSelectors:$L,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Se(UL),zt(0,"span",0),se(1),Ne(2,"span",1),se(3,1),Ge(),se(4,2),zt(5,"span",2)(6,"span",3)),i&2&&z("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();function GL(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var Hf=pt;var Hb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[uo,ye]})}return t})();function YS(t){return Error(`Unable to find icon with the name "${t}"`)}function WL(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function ZS(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function QS(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Sr=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},KS=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Sr(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(Ht.HTML,r);if(!s)throw QS(r);let a=bs(s);return this._addSvgIconConfig(e,i,new Sr("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Sr(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(Ht.HTML,i);if(!o)throw QS(i);let s=bs(o);return this._addSvgIconSetConfig(e,new Sr("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(Ht.RESOURCE_URL,e);if(!i)throw ZS(e);let r=this._cachedIconsByUrl.get(i);return r?Q(zf(r)):this._loadSvgIconFromConfig(new Sr(e,null)).pipe(ct(o=>this._cachedIconsByUrl.set(i,o)),ee(o=>zf(o)))}getNamedSvgIcon(e,i=""){let r=XS(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):pc(YS(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?Q(zf(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(ee(i=>zf(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return Q(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(di(a=>{let l=`Loading icon set URL: ${this._sanitizer.sanitize(Ht.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(l)),Q(null)})));return jo(o).pipe(ee(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw YS(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(ct(i=>e.svgText=i),ee(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?Q(null):this._fetchIcon(e).pipe(ct(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(bs("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(bs("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw WL();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(Ht.RESOURCE_URL,i);if(!s)throw ZS(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let c=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(ee(l=>bs(l)),Ni(()=>this._inProgressUrlFetches.delete(s)),vc());return this._inProgressUrlFetches.set(s,c),c}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(XS(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return qL(o)?new Sr(o.url,null,o.options):new Sr(o,null)}}static \u0275fac=function(i){return new(i||t)(te(dn,8),te(_l),te(K,8),te(on))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function zf(t){return t.cloneNode(!0)}function XS(t,n){return t+":"+n}function qL(t){return!!(t.url&&t.options)}var YL=["*"],ZL=new y("MAT_ICON_DEFAULT_OPTIONS"),QL=new y("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(K),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),JS=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],XL=JS.map(t=>`[${t}]`).join(", "),KL=/^url\(['"]?#(.*?)['"]?\)$/,An=(()=>{class t{_elementRef=d(P);_iconRegistry=d(KS);_location=d(QL);_errorHandler=d(on);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=ve.EMPTY;constructor(){let e=d(new Jn("aria-hidden"),{optional:!0}),i=d(ZL,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(XL),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)JS.forEach(s=>{let a=i[o],c=a.getAttribute(s),l=c?c.match(KL):null;if(l){let u=r.get(a);u||(u=[],r.set(a,u)),u.push({name:s,value:l[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Ke(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(Y("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),It(r.color?"mat-"+r.color:""),z("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",V],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:YL,decls:1,vars:0,template:function(i,r){i&1&&(Se(),se(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),Uf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[ye]})}return t})();var it=class t{baseUrl=Kt.apiUrl;http=d(dn);cart=N(null);itemCount=Ct(()=>this.cart()?.items.reduce((n,e)=>n+e.quantityItems,0)??0);selectedDelivery=N(null);totals=Ct(()=>{let n=this.cart(),e=this.selectedDelivery();if(!n)return null;let i=n.items.reduce((s,a)=>s+a.price*a.quantityItems,0),r=e?e.price:0,o=0;return{subtotal:i,shipping:r,discount:o,total:i+r-o}});getCart(){return this.http.get(this.baseUrl+"shoppingcart",{withCredentials:!0}).pipe(ee(n=>(this.cart.set(n),n)))}setCart(n){return this.http.post(this.baseUrl+"shoppingcart",n,{withCredentials:!0}).subscribe({next:e=>this.cart.set(e),error:e=>console.log(e)})}addItemtoCart(n,e=1){let i=this.cart()??this.createCart(),r;"productId"in n?r=n:r=this.mapProductToCartItem(n),i.items=this.addOrUpdateItem(i.items,r,e),this.setCart(i)}removeItemFromCart(n,e=1){let i=this.cart();if(!i)return;let r=i.items.findIndex(o=>o.productId===n);r!==-1&&(i.items[r].quantityItems>e?i.items[r].quantityItems-=e:i.items.splice(r,1),i.items.length===0?this.deletecart():this.setCart(i))}deletecart(){this.http.delete(this.baseUrl+"shoppingcart",{withCredentials:!0}).subscribe({next:()=>this.cart.set(null)})}addOrUpdateItem(n,e,i){let r=n.findIndex(o=>o.productId===e.productId);return r===-1?(e.quantityItems=i,n.push(e)):n[r].quantityItems+=i,n}mapProductToCartItem(n){return{productId:n.id,productName:n.name,price:n.price,quantityItems:0,imageUrl:n.imageUrl,brand:n.brand,category:n.category}}isProduct(n){return n.id!==void 0}createCart(){return{id:0,buyerId:"",items:[]}}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};function e2(t,n){if(t&1){let e=dt();f(0,"mat-card",0)(1,"div",1),M(2,"img",2),h(),f(3,"mat-card-content",3)(4,"h2",4),v(5),h(),f(6,"p",5),v(7),Ee(8,"currency"),h()(),f(9,"mat-card-actions",6),R("click",function(r){return r.stopPropagation()}),f(10,"button",7),R("click",function(){Me(e);let r=S();return Te(r.cartService.addItemtoCart(r.product))}),f(11,"mat-icon"),v(12,"add_shopping_cart"),h(),v(13," Add To Cart "),h()()()}if(t&2){let e=S();D("routerLink",Xn("/shop/",e.product.id)),p(2),D("src",Xt(e.product.imageUrl),Zn)("alt",Xt(e.product.name)),p(3),Z(e.product.name),p(2),Z(Sn(8,8,e.product.price))}}var $f=class t{product;cartService=d(it);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-product-item"]],inputs:{product:"product"},decls:1,vars:1,consts:[["appearance","raised",1,"product-card","max-w-xl","mx-auto","pt-6","px-2","flex","flex-col","justify-between","h-full",3,"routerLink"],[1,"h-48","flex","items-center","justify-center"],[1,"rounded-t-lg",3,"src","alt"],[1,"mt-2"],[1,"text-sm","font-medium","uppercase","line-clamp-2"],[1,"font-semibold"],[3,"click"],["mat-stroked-button","",1,"w-full",3,"click"]],template:function(e,i){e&1&&F(0,e2,14,10,"mat-card",0),e&2&&L(i.product?0:-1)},dependencies:[co,lS,dS,pt,An,nt,kt],styles:[".product-card[_ngcontent-%COMP%]{transition:transform .2s,box-shadow .2s}.product-card[_ngcontent-%COMP%]:hover{transform:translateY(-10px);box-shadow:0 20px 30px #0003;cursor:pointer}mat-card[_ngcontent-%COMP%]{background-color:#fff!important}"]})};var t2=20,Es=(()=>{class t{_ngZone=d(j);_platform=d(Ce);_renderer=d(Ot).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new I;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=t2){return this._platform.isBrowser?new ce(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(ru(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):Q()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(xe(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._scrollableContainsElement(o,e)&&i.push(o)}),i}_scrollableContainsElement(e,i){let r=Cn(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var n2=20,Ir=(()=>{class t{_platform=d(Ce);_listeners;_viewportSize=null;_change=new I;_document=d(K);constructor(){let e=d(j),i=d(Ot).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=n2){return e>0?this._change.pipe(ru(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ds=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({})}return t})(),zb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[ye,Ds,ye,Ds]})}return t})();var Ql=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},wi=class extends Ql{component;viewContainerRef;injector;projectableNodes;bindings;constructor(n,e,i,r,o){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},Vn=class extends Ql{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Ub=class extends Ql{element;constructor(n){super(),this.element=n instanceof P?n.nativeElement:n}},mo=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof wi)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Vn)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Ub)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},Xl=class extends mo{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(qi,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||X.NULL,o=r.get(ze,i.injector);e=Pm(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var nr=(()=>{class t extends mo{_moduleRef=d(qi,{optional:!0});_document=d(K);_viewContainerRef=d(Nt);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new H;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[_e]})}return t})(),Ss=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({})}return t})();var eI=FS();function Va(t){return new Gf(t.get(Ir),t.get(K))}var Gf=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=Tt(-this._previousScrollPosition.left),n.style.top=Tt(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),eI&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),eI&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function aI(t,n){return new Wf(t.get(Es),t.get(j),t.get(Ir),n)}var Wf=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(xe(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var Kl=class{enable(){}disable(){}attach(){}};function $b(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,s=t.left>e.right;return i||r||o||s})}function tI(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,s=t.right>e.right;return i||r||o||s})}function Mr(t,n){return new qf(t.get(Es),t.get(Ir),t.get(j),n)}var qf=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();$b(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},cI=(()=>{class t{_injector=d(X);constructor(){}noop=()=>new Kl;close=e=>aI(this._injector,e);block=()=>Va(this._injector);reposition=e=>Mr(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Di=class{positionStrategy;scrollStrategy=new Kl;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Yf=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var lI=(()=>{class t{_attachedOverlays=[];_document=d(K);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),dI=(()=>{class t extends lI{_ngZone=d(j);_renderer=d(Ot).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),uI=(()=>{class t extends lI{_platform=d(Ce);_ngZone=d(j);_renderer=d(Ot).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=en(e)};_clickListener=e=>{let i=en(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],c=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,c))){if(nI(a.overlayElement,i)||nI(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>c.next(e)):c.next(e)}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function nI(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var mI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),Xf=(()=>{class t{_platform=d(Ce);_containerElement;_document=d(K);_styleLoader=d(qe);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Nb()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),Nb()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(mI)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Gb=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Wb(t){return t&&t.nodeType===1}var Ba=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new I;_attachments=new I;_detachments=new I;_positionStrategy;_scrollStrategy;_locationChanges=ve.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new I;_outsidePointerEvents=new I;_afterNextRenderRef;constructor(n,e,i,r,o,s,a,c,l,u=!1,m,g){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=u,this._injector=m,this._renderer=g,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=bt(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=b(b({},this._config),n),this._updateElementSize()}setDirection(n){this._config=J(b({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=Tt(this._config.width),n.height=Tt(this._config.height),n.minWidth=Tt(this._config.minWidth),n.minHeight=Tt(this._config.minHeight),n.maxWidth=Tt(this._config.maxWidth),n.maxHeight=Tt(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Wb(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Gb(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=Oa(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=bt(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},iI="cdk-overlay-connected-position-bounding-box",i2=/([A-Za-z%]+)$/;function ks(t,n){return new Zf(n,t.get(Ir),t.get(K),t.get(Ce),t.get(Xf))}var Zf=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new I;_resizeSubscription=ve.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(iI),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let c=this._getOriginPoint(n,r,a),l=this._getOverlayPoint(c,e,a),u=this._getOverlayFit(l,e,i,a);if(u.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,c);return}if(this._canFitWithFlexibleDimensions(u,l,i)){o.push({position:a,origin:c,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(c,a)});continue}(!s||s.overlayFit.visibleArea<u.visibleArea)&&(s={overlayFit:u,overlayPoint:l,originPoint:c,position:a,overlayRect:e})}if(o.length){let a=null,c=-1;for(let l of o){let u=l.boundingBoxRect.width*l.boundingBoxRect.height*(l.position.weight||1);u>c&&(c=u,a=l)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&Is(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(iI),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof P?this._origin.nativeElement:Wb(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=oI(e),{x:s,y:a}=n,c=this._getOffset(r,"x"),l=this._getOffset(r,"y");c&&(s+=c),l&&(a+=l);let u=0-s,m=s+o.width-i.width,g=0-a,_=a+o.height-i.height,x=this._subtractOverflows(o.width,u,m),O=this._subtractOverflows(o.height,g,_),B=x*O;return{visibleArea:B,isCompletelyWithinViewport:o.width*o.height===B,fitsInViewportVertically:O===o.height,fitsInViewportHorizontally:x==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=rI(this._overlayRef.getConfig().minHeight),a=rI(this._overlayRef.getConfig().minWidth),c=n.fitsInViewportVertically||s!=null&&s<=r,l=n.fitsInViewportHorizontally||a!=null&&a<=o;return c&&l}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=oI(e),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),c=Math.max(o.top-i.top-n.y,0),l=Math.max(o.left-i.left-n.x,0),u=0,m=0;return r.width<=o.width?u=l||-s:u=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?m=c||-a:m=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:u,y:m},{x:n.x+u,y:n.y+m}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!r2(this._lastScrollVisibility,i)){let r=new Yf(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let _=Math.min(i.bottom-n.y+i.top,n.y),x=this._lastBoundingBoxSize.height;o=_*2,s=n.y-_,o>x&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-x/2)}let c=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,l=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,u,m,g;if(l)g=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),u=n.x-this._getViewportMarginStart();else if(c)m=n.x,u=i.right-n.x-this._getViewportMarginEnd();else{let _=Math.min(i.right-n.x+i.left,n.x),x=this._lastBoundingBoxSize.width;u=_*2,m=n.x-_,u>x&&!this._isInitialRender&&!this._growAfterOpen&&(m=n.x-x/2)}return{top:s,left:m,bottom:a,right:g,width:u,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=Tt(i.width),r.height=Tt(i.height),r.top=Tt(i.top)||"auto",r.bottom=Tt(i.bottom)||"auto",r.left=Tt(i.left)||"auto",r.right=Tt(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=Tt(o)),s&&(r.maxWidth=Tt(s))}this._lastBoundingBoxSize=i,Is(this._boundingBox.style,r)}_resetBoundingBoxStyles(){Is(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){Is(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let u=this._viewportRuler.getViewportScrollPosition();Is(i,this._getExactOverlayY(e,n,u)),Is(i,this._getExactOverlayX(e,n,u))}else i.position="static";let a="",c=this._getOffset(e,"x"),l=this._getOffset(e,"y");c&&(a+=`translateX(${c}px) `),l&&(a+=`translateY(${l}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=Tt(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=Tt(s.maxWidth):o&&(i.maxWidth="")),Is(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=Tt(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=Tt(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:tI(n,i),isOriginOutsideView:$b(n,i),isOverlayClipped:tI(e,i),isOverlayOutsideView:$b(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Oa(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof P)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function Is(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function rI(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(i2);return!e||e==="px"?parseFloat(n):null}return t||null}function oI(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function r2(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var sI="cdk-global-overlay-wrapper";function fo(t){return new Qf}var Qf=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(sI),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,c=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),l=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),u=this._xPosition,m=this._xOffset,g=this._overlayRef.getConfig().direction==="rtl",_="",x="",O="";c?O="flex-start":u==="center"?(O="center",g?x=m:_=m):g?u==="left"||u==="end"?(O="flex-end",_=m):(u==="right"||u==="start")&&(O="flex-start",x=m):u==="left"||u==="start"?(O="flex-start",_=m):(u==="right"||u==="end")&&(O="flex-end",x=m),n.position=this._cssPosition,n.marginLeft=c?"0":_,n.marginTop=l?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=c?"0":x,e.justifyContent=O,e.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(sI),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},fI=(()=>{class t{_injector=d(X);constructor(){}global(){return fo()}flexibleConnectedTo(e){return ks(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Jl=new y("OVERLAY_DEFAULT_CONFIG");function Ei(t,n){t.get(qe).load(mI);let e=t.get(Xf),i=t.get(K),r=t.get(Ie),o=t.get(En),s=t.get(Jt),a=t.get(we,null,{optional:!0})||t.get(Ot).createRenderer(null,null),c=new Di(n),l=t.get(Jl,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||s.value,"showPopover"in i.body?c.usePopover=n?.usePopover??l:c.usePopover=!1;let u=i.createElement("div"),m=i.createElement("div");u.id=r.getId("cdk-overlay-"),u.classList.add("cdk-overlay-pane"),m.appendChild(u),c.usePopover&&(m.setAttribute("popover","manual"),m.classList.add("cdk-overlay-popover"));let g=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return Wb(g)?g.after(m):g?.type==="parent"?g.element.appendChild(m):e.getContainerElement().appendChild(m),new Ba(new Xl(u,o,t),m,u,c,t.get(j),t.get(dI),i,t.get(Zi),t.get(uI),n?.disableAnimations??t.get(Yc,null,{optional:!0})==="NoopAnimations",t.get(ze),a)}var hI=(()=>{class t{scrollStrategies=d(cI);_positionBuilder=d(fI);_injector=d(X);constructor(){}create(e){return Ei(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),o2=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],s2=new y("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(X);return()=>Mr(t)}}),ja=(()=>{class t{elementRef=d(P);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),pI=new y("cdk-connected-overlay-default-config"),Kf=(()=>{class t{_dir=d(Jt,{optional:!0});_injector=d(X);_overlayRef;_templatePortal;_backdropSubscription=ve.EMPTY;_attachSubscription=ve.EMPTY;_detachSubscription=ve.EMPTY;_positionSubscription=ve.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d(s2);_ngZone=d(j);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new H;positionChange=new H;attach=new H;detach=new H;overlayKeydown=new H;overlayOutsideClick=new H;constructor(){let e=d(mt),i=d(Nt),r=d(pI,{optional:!0}),o=d(Jl,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Vn(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=o2);let e=this._overlayRef=Ei(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!xt(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=en(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new Di({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=ks(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof ja?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof ja?this.origin.elementRef.nativeElement:this.origin instanceof P?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(Mp(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",V],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",V],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",V],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",V],push:[2,"cdkConnectedOverlayPush","push",V],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",V],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",V],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[$e]})}return t})(),Tr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({providers:[hI],imports:[ye,Ss,zb,zb]})}return t})();function a2(t,n){}var ho=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext};var Yb=(()=>{class t extends mo{_elementRef=d(P);_focusTrapFactory=d(Mb);_config;_interactivityChecker=d(Pa);_ngZone=d(j);_focusMonitor=d(Mn);_renderer=d(we);_changeDetectorRef=d(ue);_injector=d(X);_platform=d(Ce);_document=d(K);_portalOutlet;_focusTrapped=new I;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(ho,{optional:!0})||new ho,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),s(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),s=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||bt(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=Dr(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=Dr();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=Dr()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&je(nr,7),i&2){let o;U(o=$())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&Y("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[_e],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&Be(0,a2,0,0,"ng-template",0)},dependencies:[nr],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2})}return t})(),ed=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new I;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!xt(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},c2=new y("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(X);return()=>Va(t)}}),l2=new y("DialogData"),d2=new y("DefaultDialogConfig");function u2(t){let n=N(t),e=new H;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var gI=(()=>{class t{_injector=d(X);_defaultOptions=d(d2,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(Xf);_idGenerator=d(Ie);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new I;_afterOpenedAtThisLevel=new I;_ariaHiddenElements=new Map;_scrollStrategy=d(c2);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=li(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(Je(void 0)));constructor(){}open(e,i){let r=this._defaultOptions||new ho;i=b(b({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),s=Ei(this._injector,o),a=new ed(s,i),c=this._attachContainer(s,a,i);if(a.containerInstance=c,!this.openDialogs.length){let l=this._overlayContainer.getContainerElement();c._focusTrapped?c._focusTrapped.pipe(Ke(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(l)}):this._hideNonDialogContentFromAssistiveTechnology(l)}return this._attachDialogContent(e,a,c,i),this.openDialogs.push(a),a.closed.subscribe(()=>this._removeOpenDialog(a,!0)),this.afterOpened.next(a),a}closeAll(){qb(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){qb(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),qb(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new Di({positionStrategy:e.positionStrategy||fo().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,s=[{provide:ho,useValue:r},{provide:ed,useValue:i},{provide:Ba,useValue:e}],a;r.container?typeof r.container=="function"?a=r.container:(a=r.container.type,s.push(...r.container.providers(r))):a=Yb;let c=new wi(a,r.viewContainerRef,X.create({parent:o||this._injector,providers:s}));return e.attach(c).instance}_attachDialogContent(e,i,r,o){if(e instanceof mt){let s=this._createInjector(o,i,r,void 0),a={$implicit:o.data,dialogRef:i};o.templateContext&&(a=b(b({},a),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new Vn(e,null,a,s))}else{let s=this._createInjector(o,i,r,this._injector),a=r.attachComponentPortal(new wi(e,o.viewContainerRef,s));i.componentRef=a,i.componentInstance=a.instance}}_createInjector(e,i,r,o){let s=e.injector||e.viewContainerRef?.injector,a=[{provide:l2,useValue:e.data},{provide:ed,useValue:i}];return e.providers&&(typeof e.providers=="function"?a.push(...e.providers(i,e,r)):a.push(...e.providers)),e.direction&&(!s||!s.get(Jt,null,{optional:!0}))&&a.push({provide:Jt,useValue:u2(e.direction)}),X.create({parent:s||o,providers:a})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,s)=>{o?s.setAttribute("aria-hidden",o):s.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function qb(t,n){let e=t.length;for(;e--;)n(t[e])}function m2(t,n){}var eh=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration},Zb="mdc-dialog--open",_I="mdc-dialog--opening",vI="mdc-dialog--closing",f2=150,h2=75,p2=(()=>{class t extends Yb{_animationStateChanged=new H;_animationsEnabled=!ke();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?yI(this._config.enterAnimationDuration)??f2:0;_exitAnimationDuration=this._animationsEnabled?yI(this._config.exitAnimationDuration)??h2:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(bI,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(_I,Zb)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(Zb),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(Zb),this._animationsEnabled?(this._hostElement.style.setProperty(bI,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(vI)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(_I,vI)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275cmp=E({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(ft("id",r._config.id),Y("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),z("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[_e],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(f(0,"div",0)(1,"div",1),Be(2,m2,0,0,"ng-template",2),h()())},dependencies:[nr],styles:[`.mat-mdc-dialog-container {
  width: 100%;
  height: 100%;
  display: block;
  box-sizing: border-box;
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  outline: 0;
}

.cdk-overlay-pane.mat-mdc-dialog-panel {
  max-width: var(--mat-dialog-container-max-width, 560px);
  min-width: var(--mat-dialog-container-min-width, 280px);
}
@media (max-width: 599px) {
  .cdk-overlay-pane.mat-mdc-dialog-panel {
    max-width: var(--mat-dialog-container-small-max-width, calc(100vw - 32px));
  }
}

.mat-mdc-dialog-inner-container {
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
  box-sizing: border-box;
  height: 100%;
  opacity: 0;
  transition: opacity linear var(--mat-dialog-transition-duration, 0ms);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
}
.mdc-dialog--closing .mat-mdc-dialog-inner-container {
  transition: opacity 75ms linear;
  transform: none;
}
.mdc-dialog--open .mat-mdc-dialog-inner-container {
  opacity: 1;
}
._mat-animation-noopable .mat-mdc-dialog-inner-container {
  transition: none;
}

.mat-mdc-dialog-surface {
  display: flex;
  flex-direction: column;
  flex-grow: 0;
  flex-shrink: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  position: relative;
  overflow-y: auto;
  outline: 0;
  transform: scale(0.8);
  transition: transform var(--mat-dialog-transition-duration, 0ms) cubic-bezier(0, 0, 0.2, 1);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  box-shadow: var(--mat-dialog-container-elevation-shadow, none);
  border-radius: var(--mat-dialog-container-shape, var(--mat-sys-corner-extra-large, 4px));
  background-color: var(--mat-dialog-container-color, var(--mat-sys-surface, white));
}
[dir=rtl] .mat-mdc-dialog-surface {
  text-align: right;
}
.mdc-dialog--open .mat-mdc-dialog-surface, .mdc-dialog--closing .mat-mdc-dialog-surface {
  transform: none;
}
._mat-animation-noopable .mat-mdc-dialog-surface {
  transition: none;
}
.mat-mdc-dialog-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 2px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}

.mat-mdc-dialog-title {
  display: block;
  position: relative;
  flex-shrink: 0;
  box-sizing: border-box;
  margin: 0 0 1px;
  padding: var(--mat-dialog-headline-padding, 6px 24px 13px);
}
.mat-mdc-dialog-title::before {
  display: inline-block;
  width: 0;
  height: 40px;
  content: "";
  vertical-align: 0;
}
[dir=rtl] .mat-mdc-dialog-title {
  text-align: right;
}
.mat-mdc-dialog-container .mat-mdc-dialog-title {
  color: var(--mat-dialog-subhead-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-dialog-subhead-font, var(--mat-sys-headline-small-font, inherit));
  line-height: var(--mat-dialog-subhead-line-height, var(--mat-sys-headline-small-line-height, 1.5rem));
  font-size: var(--mat-dialog-subhead-size, var(--mat-sys-headline-small-size, 1rem));
  font-weight: var(--mat-dialog-subhead-weight, var(--mat-sys-headline-small-weight, 400));
  letter-spacing: var(--mat-dialog-subhead-tracking, var(--mat-sys-headline-small-tracking, 0.03125em));
}

.mat-mdc-dialog-content {
  display: block;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  overflow: auto;
  max-height: 65vh;
}
.mat-mdc-dialog-content > :first-child {
  margin-top: 0;
}
.mat-mdc-dialog-content > :last-child {
  margin-bottom: 0;
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  color: var(--mat-dialog-supporting-text-color, var(--mat-sys-on-surface-variant, rgba(0, 0, 0, 0.6)));
  font-family: var(--mat-dialog-supporting-text-font, var(--mat-sys-body-medium-font, inherit));
  line-height: var(--mat-dialog-supporting-text-line-height, var(--mat-sys-body-medium-line-height, 1.5rem));
  font-size: var(--mat-dialog-supporting-text-size, var(--mat-sys-body-medium-size, 1rem));
  font-weight: var(--mat-dialog-supporting-text-weight, var(--mat-sys-body-medium-weight, 400));
  letter-spacing: var(--mat-dialog-supporting-text-tracking, var(--mat-sys-body-medium-tracking, 0.03125em));
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  padding: var(--mat-dialog-content-padding, 20px 24px);
}
.mat-mdc-dialog-container-with-actions .mat-mdc-dialog-content {
  padding: var(--mat-dialog-with-actions-content-padding, 20px 24px 0);
}
.mat-mdc-dialog-container .mat-mdc-dialog-title + .mat-mdc-dialog-content {
  padding-top: 0;
}

.mat-mdc-dialog-actions {
  display: flex;
  position: relative;
  flex-shrink: 0;
  flex-wrap: wrap;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  margin: 0;
  border-top: 1px solid transparent;
  padding: var(--mat-dialog-actions-padding, 16px 24px);
  justify-content: var(--mat-dialog-actions-alignment, flex-end);
}
@media (forced-colors: active) {
  .mat-mdc-dialog-actions {
    border-top-color: CanvasText;
  }
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-start, .mat-mdc-dialog-actions[align=start] {
  justify-content: start;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-center, .mat-mdc-dialog-actions[align=center] {
  justify-content: center;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-end, .mat-mdc-dialog-actions[align=end] {
  justify-content: flex-end;
}
.mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
.mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
[dir=rtl] .mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}

.mat-mdc-dialog-component-host {
  display: contents;
}
`],encapsulation:2})}return t})(),bI="--mat-dialog-transition-duration";function yI(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?jn(t.substring(0,t.length-2)):t.endsWith("s")?jn(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var Jf=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(Jf||{}),td=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new Ai(1);_beforeClosed=new Ai(1);_result;_closeFallbackTimeout;_state=Jf.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(xe(r=>r.state==="opened"),Ke(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(xe(r=>r.state==="closed"),Ke(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),fn(this.backdropClick(),this.keydownEvents().pipe(xe(r=>r.keyCode===27&&!this.disableClose&&!xt(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),g2(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(xe(i=>i.state==="closing"),Ke(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=Jf.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=Jf.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function g2(t,n,e){return t._closeInteractionType=n,t.close(e)}var Qb=new y("MatMdcDialogData"),_2=new y("mat-mdc-dialog-default-options"),v2=new y("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(X);return()=>Va(t)}}),CI=(()=>{class t{_defaultOptions=d(_2,{optional:!0});_scrollStrategy=d(v2);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(Ie);_injector=d(X);_dialog=d(gI);_animationsDisabled=ke();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new I;_afterOpenedAtThisLevel=new I;dialogConfigClass=eh;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=li(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(Je(void 0)));constructor(){this._dialogRefConstructor=td,this._dialogContainerType=p2,this._dialogDataToken=Qb}open(e,i){let r;i=b(b({},this._defaultOptions||new eh),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,J(b({},i),{positionStrategy:fo(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:ho,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(s,a,c)=>(r=new this._dialogRefConstructor(s,i,c),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:c},{provide:this._dialogDataToken,useValue:a.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let s=this.openDialogs.indexOf(r);s>-1&&(this.openDialogs.splice(s,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ha=(()=>{class t{get vertical(){return this._vertical}set vertical(e){this._vertical=At(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=At(e)}_inset=!1;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(Y("aria-orientation",r.vertical?"vertical":"horizontal"),z("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
  display: block;
  margin: 0;
  border-top-style: solid;
  border-top-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-top-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-vertical {
  border-top: 0;
  border-right-style: solid;
  border-right-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-right-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-inset {
  margin-left: 80px;
}
[dir=rtl] .mat-divider.mat-divider-inset {
  margin-left: auto;
  margin-right: 80px;
}
`],encapsulation:2,changeDetection:0})}return t})();var po=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new I;constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}deselect(...n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}setSelection(...n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var Xb=(()=>{class t{_listeners=[];notify(e,i){for(let r of this._listeners)r(e,i)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(i=>e!==i)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var MI=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(ie(we),ie(P))};static \u0275dir=T({type:t})}return t})(),TI=(()=>{class t extends MI{static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275dir=T({type:t,features:[_e]})}return t})(),ki=new y("");var b2={provide:ki,useExisting:Rt(()=>ir),multi:!0};function y2(){let t=ei()?ei().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var C2=new y(""),ir=(()=>{class t extends MI{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!y2())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(ie(we),ie(P),ie(C2,8))};static \u0275dir=T({type:t,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&R("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[Re([b2]),_e]})}return t})();function ty(t){return t==null||ny(t)===0}function ny(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var _o=new y(""),dd=new y(""),x2=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Si=class{static min(n){return AI(n)}static max(n){return w2(n)}static required(n){return D2(n)}static requiredTrue(n){return E2(n)}static email(n){return S2(n)}static minLength(n){return I2(n)}static maxLength(n){return k2(n)}static pattern(n){return M2(n)}static nullValidator(n){return nh()}static compose(n){return LI(n)}static composeAsync(n){return BI(n)}};function AI(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function w2(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function D2(t){return ty(t.value)?{required:!0}:null}function E2(t){return t.value===!0?null:{required:!0}}function S2(t){return ty(t.value)||x2.test(t.value)?null:{email:!0}}function I2(t){return n=>{let e=n.value?.length??ny(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function k2(t){return n=>{let e=n.value?.length??ny(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function M2(t){if(!t)return nh;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(ty(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function nh(t){return null}function RI(t){return t!=null}function OI(t){return Jr(t)?rt(t):t}function NI(t){let n={};return t.forEach(e=>{n=e!=null?b(b({},n),e):n}),Object.keys(n).length===0?null:n}function PI(t,n){return n.map(e=>e(t))}function T2(t){return!t.validate}function FI(t){return t.map(n=>T2(n)?n:e=>n.validate(e))}function LI(t){if(!t)return null;let n=t.filter(RI);return n.length==0?null:function(e){return NI(PI(e,n))}}function iy(t){return t!=null?LI(FI(t)):null}function BI(t){if(!t)return null;let n=t.filter(RI);return n.length==0?null:function(e){let i=PI(e,n).map(OI);return jo(i).pipe(ee(NI))}}function ry(t){return t!=null?BI(FI(t)):null}function xI(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function jI(t){return t._rawValidators}function VI(t){return t._rawAsyncValidators}function Kb(t){return t?Array.isArray(t)?t:[t]:[]}function ih(t,n){return Array.isArray(t)?t.includes(n):t===n}function wI(t,n){let e=Kb(n);return Kb(t).forEach(r=>{ih(e,r)||e.push(r)}),e}function DI(t,n){return Kb(n).filter(e=>!ih(t,e))}var rh=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=iy(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=ry(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Ii=class extends rh{name;get formDirective(){return null}get path(){return null}},xn=class extends rh{_parent=null;name=null;valueAccessor=null},oh=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var ri=(()=>{class t extends oh{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(ie(xn,2))};static \u0275dir=T({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&z("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[_e]})}return t})(),Wa=(()=>{class t extends oh{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(ie(Ii,10))};static \u0275dir=T({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&z("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[_e]})}return t})();var nd="VALID",th="INVALID",za="PENDING",id="DISABLED",go=class{},sh=class extends go{value;source;constructor(n,e){super(),this.value=n,this.source=e}},od=class extends go{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},sd=class extends go{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},Ua=class extends go{status;source;constructor(n,e){super(),this.status=n,this.source=e}},ah=class extends go{source;constructor(n){super(),this.source=n}},cd=class extends go{source;constructor(n){super(),this.source=n}};function oy(t){return(uh(t)?t.validators:t)||null}function A2(t){return Array.isArray(t)?iy(t):t||null}function sy(t,n){return(uh(n)?n.asyncValidators:t)||null}function R2(t){return Array.isArray(t)?ry(t):t||null}function uh(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function HI(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new A(1e3,"");if(!i[e])throw new A(1001,"")}function zI(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new A(-1002,"")})}var $a=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Oe(this.statusReactive)}set status(n){Oe(()=>this.statusReactive.set(n))}_status=Ct(()=>this.statusReactive());statusReactive=N(void 0);get valid(){return this.status===nd}get invalid(){return this.status===th}get pending(){return this.status===za}get disabled(){return this.status===id}get enabled(){return this.status!==id}errors;get pristine(){return Oe(this.pristineReactive)}set pristine(n){Oe(()=>this.pristineReactive.set(n))}_pristine=Ct(()=>this.pristineReactive());pristineReactive=N(!0);get dirty(){return!this.pristine}get touched(){return Oe(this.touchedReactive)}set touched(n){Oe(()=>this.touchedReactive.set(n))}_touched=Ct(()=>this.touchedReactive());touchedReactive=N(!1);get untouched(){return!this.touched}_events=new I;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(wI(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(wI(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(DI(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(DI(n,this._rawAsyncValidators))}hasValidator(n){return ih(this._rawValidators,n)}hasAsyncValidator(n){return ih(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(J(b({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new sd(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new sd(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(J(b({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new od(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new od(!0,i))}markAsPending(n={}){this.status=za;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Ua(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(J(b({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=id,this.errors=null,this._forEachChild(r=>{r.disable(J(b({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new sh(this.value,i)),this._events.next(new Ua(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(J(b({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=nd,this._forEachChild(i=>{i.enable(J(b({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(J(b({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===nd||this.status===za)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new sh(this.value,e)),this._events.next(new Ua(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(J(b({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?id:nd}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=za,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=OI(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new Ua(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new H,this.statusChanges=new H}_calculateStatus(){return this._allControlsDisabled()?id:this.errors?th:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(za)?za:this._anyControlsHaveStatus(th)?th:nd}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new od(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new sd(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){uh(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=A2(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=R2(this._rawAsyncValidators)}},Ga=class extends $a{constructor(n,e,i){super(oy(e),sy(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){return this.controls[n]?this.controls[n]:(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this.controls.hasOwnProperty(n)&&this.controls[n].enabled}setValue(n,e={}){zI(this,!0,n),Object.keys(n).forEach(i=>{HI(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this.controls[i];r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,J(b({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new cd(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return this.controls.hasOwnProperty(n)?this.controls[n]:null}};var Jb=class extends Ga{};var qa=new y("",{factory:()=>mh}),mh="always";function UI(t,n){return[...n.path,t]}function ld(t,n,e=mh){ay(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),N2(t,n),F2(t,n),P2(t,n),O2(t,n)}function ch(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),dh(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function lh(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function O2(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function ay(t,n){let e=jI(t);n.validator!==null?t.setValidators(xI(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=VI(t);n.asyncValidator!==null?t.setAsyncValidators(xI(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();lh(n._rawValidators,r),lh(n._rawAsyncValidators,r)}function dh(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=jI(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=VI(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return lh(n._rawValidators,i),lh(n._rawAsyncValidators,i),e}function N2(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&$I(t,n)})}function P2(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&$I(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function $I(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function F2(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function GI(t,n){t==null,ay(t,n)}function L2(t,n){return dh(t,n)}function cy(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function B2(t){return Object.getPrototypeOf(t.constructor)===TI}function WI(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function ly(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===ir?e=o:B2(o)?i=o:r=o}),r||i||e||null}function j2(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var V2={provide:Ii,useExisting:Rt(()=>Ms)},rd=Promise.resolve(),Ms=(()=>{class t extends Ii{callSetDisabledState;get submitted(){return Oe(this.submittedReactive)}_submitted=Ct(()=>this.submittedReactive());submittedReactive=N(!1);_directives=new Set;form;ngSubmit=new H;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Ga({},iy(e),ry(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){rd.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),ld(e.control,e,this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){rd.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){rd.then(()=>{let i=this._findContainer(e.path),r=new Ga({});GI(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){rd.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){rd.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),WI(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new ah(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(ie(_o,10),ie(dd,10),ie(qa,8))};static \u0275dir=T({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&R("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Re([V2]),_e]})}return t})();function EI(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function SI(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var ad=class extends $a{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(oy(e),sy(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),uh(e)&&(e.nonNullable||e.initialValueIsDefault)&&(SI(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new cd(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){EI(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){EI(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){SI(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var H2=t=>t instanceof ad;var z2={provide:xn,useExisting:Rt(()=>Ts)},II=Promise.resolve(),Ts=(()=>{class t extends xn{_changeDetectorRef;callSetDisabledState;control=new ad;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new H;constructor(e,i,r,o,s,a){super(),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=ly(this,o)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),cy(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){ld(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){II.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&V(i);II.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?UI(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(ie(Ii,9),ie(_o,10),ie(dd,10),ie(ki,10),ie(ue,8),ie(qa,8))};static \u0275dir=T({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Re([z2]),_e,$e]})}return t})();var Ya=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})(),U2={provide:ki,useExisting:Rt(()=>dy),multi:!0},dy=(()=>{class t extends TI{writeValue(e){let i=e??"";this.setProperty("value",i)}registerOnChange(e){this.onChange=i=>{e(i==""?null:parseFloat(i))}}static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["input","type","number","formControlName",""],["input","type","number","formControl",""],["input","type","number","ngModel",""]],hostBindings:function(i,r){i&1&&R("input",function(s){return r.onChange(s.target.value)})("blur",function(){return r.onTouched()})},standalone:!1,features:[Re([U2]),_e]})}return t})();var ey=class extends $a{constructor(n,e,i){super(oy(e),sy(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;at(n){return this.controls[this._adjustIndex(n)]}push(n,e={}){Array.isArray(n)?n.forEach(i=>{this.controls.push(i),this._registerControl(i)}):(this.controls.push(n),this._registerControl(n)),this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}insert(n,e,i={}){this.controls.splice(n,0,e),this._registerControl(e),this.updateValueAndValidity({emitEvent:i.emitEvent})}removeAt(n,e={}){let i=this._adjustIndex(n);i<0&&(i=0),this.controls[i]&&this.controls[i]._registerOnCollectionChange(()=>{}),this.controls.splice(i,1),this.updateValueAndValidity({emitEvent:e.emitEvent})}setControl(n,e,i={}){let r=this._adjustIndex(n);r<0&&(r=0),this.controls[r]&&this.controls[r]._registerOnCollectionChange(()=>{}),this.controls.splice(r,1),e&&(this.controls.splice(r,0,e),this._registerControl(e)),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}get length(){return this.controls.length}setValue(n,e={}){zI(this,!1,n),n.forEach((i,r)=>{HI(this,!1,r),this.at(r).setValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(n.forEach((i,r)=>{this.at(r)&&this.at(r).patchValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n=[],e={}){this._forEachChild((i,r)=>{i.reset(n[r],J(b({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new cd(this))}getRawValue(){return this.controls.map(n=>n.getRawValue())}clear(n={}){this.controls.length<1||(this._forEachChild(e=>e._registerOnCollectionChange(()=>{})),this.controls.splice(0),this.updateValueAndValidity({emitEvent:n.emitEvent}))}_adjustIndex(n){return n<0?n+this.length:n}_syncPendingControls(){let n=this.controls.reduce((e,i)=>i._syncPendingControls()?!0:e,!1);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){this.controls.forEach((e,i)=>{n(e,i)})}_updateValue(){this.value=this.controls.filter(n=>n.enabled||this.disabled).map(n=>n.value)}_anyControls(n){return this.controls.some(e=>e.enabled&&n(e))}_setUpControls(){this._forEachChild(n=>this._registerControl(n))}_allControlsDisabled(){for(let n of this.controls)if(n.enabled)return!1;return this.controls.length>0||this.disabled}_registerControl(n){n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)}_find(n){return this.at(n)??null}};var $2=(()=>{class t extends Ii{callSetDisabledState;get submitted(){return Oe(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=Ct(()=>this._submittedReactive());_submittedReactive=N(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(dh(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return ld(i,e,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){ch(e.control||null,e,!1),j2(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,WI(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new ah(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(ch(i||null,e),H2(r)&&(ld(r,e,this.callSetDisabledState),e.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);GI(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&L2(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){ay(this.form,this),this._oldForm&&dh(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(ie(_o,10),ie(dd,10),ie(qa,8))};static \u0275dir=T({type:t,features:[_e,$e]})}return t})();var uy=new y(""),G2={provide:xn,useExisting:Rt(()=>my)},my=(()=>{class t extends xn{_ngModelWarningConfig;callSetDisabledState;viewModel;form;set isDisabled(e){}model;update=new H;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,s){super(),this._ngModelWarningConfig=o,this.callSetDisabledState=s,this._setValidators(e),this._setAsyncValidators(i),this.valueAccessor=ly(this,r)}ngOnChanges(e){if(this._isControlChanged(e)){let i=e.form.previousValue;i&&ch(i,this,!1),ld(this.form,this,this.callSetDisabledState),this.form.updateValueAndValidity({emitEvent:!1})}cy(e,this.viewModel)&&(this.form.setValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.form&&ch(this.form,this,!1)}get path(){return[]}get control(){return this.form}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_isControlChanged(e){return e.hasOwnProperty("form")}static \u0275fac=function(i){return new(i||t)(ie(_o,10),ie(dd,10),ie(ki,10),ie(uy,8),ie(qa,8))};static \u0275dir=T({type:t,selectors:[["","formControl",""]],inputs:{form:[0,"formControl","form"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},exportAs:["ngForm"],standalone:!1,features:[Re([G2]),_e,$e]})}return t})();var W2={provide:xn,useExisting:Rt(()=>ud)},ud=(()=>{class t extends xn{_ngModelWarningConfig;_added=!1;viewModel;control;name=null;set isDisabled(e){}model;update=new H;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,s){super(),this._ngModelWarningConfig=s,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=ly(this,o)}ngOnChanges(e){this._added||this._setUpControl(),cy(e,this.viewModel)&&(this.viewModel=this.model,this.formDirective.updateModel(this,this.model))}ngOnDestroy(){this.formDirective?.removeControl(this)}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}get path(){return UI(this.name==null?this.name:this.name.toString(),this._parent)}get formDirective(){return this._parent?this._parent.formDirective:null}_setUpControl(){this.control=this.formDirective.addControl(this),this._added=!0}static \u0275fac=function(i){return new(i||t)(ie(Ii,13),ie(_o,10),ie(dd,10),ie(ki,10),ie(uy,8))};static \u0275dir=T({type:t,selectors:[["","formControlName",""]],inputs:{name:[0,"formControlName","name"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},standalone:!1,features:[Re([W2]),_e,$e]})}return t})();var q2={provide:Ii,useExisting:Rt(()=>Ar)},Ar=(()=>{class t extends $2{form=null;ngSubmit=new H;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&R("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Re([q2]),_e]})}return t})();function Y2(t){return typeof t=="number"?t:parseFloat(t)}var Z2=(()=>{class t{_validator=nh;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):nh,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,features:[$e]})}return t})();var Q2={provide:_o,useExisting:Rt(()=>fy),multi:!0},fy=(()=>{class t extends Z2{min;inputName="min";normalizeInput=e=>Y2(e);createValidator=e=>AI(e);static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["input","type","number","min","","formControlName",""],["input","type","number","min","","formControl",""],["input","type","number","min","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&Y("min",r._enabled?r.min:null)},inputs:{min:"min"},standalone:!1,features:[Re([Q2]),_e]})}return t})();var qI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({})}return t})();function kI(t){return!!t&&(t.asyncValidators!==void 0||t.validators!==void 0||t.updateOn!==void 0)}var fh=(()=>{class t{useNonNullable=!1;get nonNullable(){let e=new t;return e.useNonNullable=!0,e}group(e,i=null){let r=this._reduceControls(e),o={};return kI(i)?o=i:i!==null&&(o.validators=i.validator,o.asyncValidators=i.asyncValidator),new Ga(r,o)}record(e,i=null){let r=this._reduceControls(e);return new Jb(r,i)}control(e,i,r){let o={};return this.useNonNullable?(kI(i)?o=i:(o.validators=i,o.asyncValidators=r),new ad(e,J(b({},o),{nonNullable:!0}))):new ad(e,i,r)}array(e,i,r){let o=e.map(s=>this._createControl(s));return new ey(o,i,r)}_reduceControls(e){let i={};return Object.keys(e).forEach(r=>{i[r]=this._createControl(e[r])}),i}_createControl(e){if(e instanceof ad)return e;if(e instanceof $a)return e;if(Array.isArray(e)){let i=e[0],r=e.length>1?e[1]:null,o=e.length>2?e[2]:null;return this.control(i,r,o)}else return this.control(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var vo=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:qa,useValue:e.callSetDisabledState??mh}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[qI]})}return t})(),Za=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:uy,useValue:e.warnOnNgModelWithFormControl??"always"},{provide:qa,useValue:e.callSetDisabledState??mh}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[qI]})}return t})();var YI=(()=>{class t{_animationsDisabled=ke();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&z("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2,changeDetection:0})}return t})();var ZI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[ye]})}return t})();var X2=["*"],K2=`.mdc-list {
  margin: 0;
  padding: 8px 0;
  list-style-type: none;
}
.mdc-list:focus {
  outline: none;
}

.mdc-list-item {
  display: flex;
  position: relative;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  align-items: stretch;
  cursor: pointer;
  padding-left: 16px;
  padding-right: 16px;
  background-color: var(--mat-list-list-item-container-color, transparent);
  border-radius: var(--mat-list-list-item-container-shape, var(--mat-sys-corner-none));
}
.mdc-list-item.mdc-list-item--selected {
  background-color: var(--mat-list-list-item-selected-container-color);
}
.mdc-list-item:focus {
  outline: 0;
}
.mdc-list-item.mdc-list-item--disabled {
  cursor: auto;
}
.mdc-list-item.mdc-list-item--with-one-line {
  height: var(--mat-list-list-item-one-line-container-height, 48px);
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__start {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-two-lines {
  height: var(--mat-list-list-item-two-line-container-height, 64px);
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-three-lines {
  height: var(--mat-list-list-item-three-line-container-height, 88px);
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--selected::before, .mdc-list-item.mdc-list-item--selected:focus::before, .mdc-list-item:not(.mdc-list-item--selected):focus::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  content: "";
  pointer-events: none;
}

a.mdc-list-item {
  color: inherit;
  text-decoration: none;
}

.mdc-list-item__start {
  fill: currentColor;
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--mat-list-list-item-leading-icon-color, var(--mat-sys-on-surface-variant));
  width: var(--mat-list-list-item-leading-icon-size, 24px);
  height: var(--mat-list-list-item-leading-icon-size, 24px);
  margin-left: 16px;
  margin-right: 32px;
}
[dir=rtl] .mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-left: 32px;
  margin-right: 16px;
}
.mdc-list-item--with-leading-icon:hover .mdc-list-item__start {
  color: var(--mat-list-list-item-hover-leading-icon-color);
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start {
  width: var(--mat-list-list-item-leading-avatar-size, 40px);
  height: var(--mat-list-list-item-leading-avatar-size, 40px);
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start, [dir=rtl] .mdc-list-item--with-leading-avatar .mdc-list-item__start {
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}

.mdc-list-item__end {
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  font-family: var(--mat-list-list-item-trailing-supporting-text-font, var(--mat-sys-label-small-font));
  line-height: var(--mat-list-list-item-trailing-supporting-text-line-height, var(--mat-sys-label-small-line-height));
  font-size: var(--mat-list-list-item-trailing-supporting-text-size, var(--mat-sys-label-small-size));
  font-weight: var(--mat-list-list-item-trailing-supporting-text-weight, var(--mat-sys-label-small-weight));
  letter-spacing: var(--mat-list-list-item-trailing-supporting-text-tracking, var(--mat-sys-label-small-tracking));
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-trailing-icon-color, var(--mat-sys-on-surface-variant));
  width: var(--mat-list-list-item-trailing-icon-size, 24px);
  height: var(--mat-list-list-item-trailing-icon-size, 24px);
}
.mdc-list-item--with-trailing-icon:hover .mdc-list-item__end {
  color: var(--mat-list-list-item-hover-trailing-icon-color);
}
.mdc-list-item.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  color: var(--mat-list-list-item-trailing-supporting-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-list-item--selected.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-selected-trailing-icon-color, var(--mat-sys-primary));
}

.mdc-list-item__content {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  align-self: center;
  flex: 1;
  pointer-events: none;
}
.mdc-list-item--with-two-lines .mdc-list-item__content, .mdc-list-item--with-three-lines .mdc-list-item__content {
  align-self: stretch;
}

.mdc-list-item__primary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  color: var(--mat-list-list-item-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-list-list-item-label-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-list-list-item-label-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-list-list-item-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-list-list-item-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-list-list-item-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-list-item:hover .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-list-item:focus .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-focus-label-text-color, var(--mat-sys-on-surface));
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text, .mdc-list-item--with-three-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}

.mdc-list-item__secondary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  display: block;
  margin-top: 0;
  color: var(--mat-list-list-item-supporting-text-color, var(--mat-sys-on-surface-variant));
  font-family: var(--mat-list-list-item-supporting-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-list-list-item-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-list-list-item-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-list-list-item-supporting-text-weight, var(--mat-sys-body-medium-weight));
  letter-spacing: var(--mat-list-list-item-supporting-text-tracking, var(--mat-sys-body-medium-tracking));
}
.mdc-list-item__secondary-text::before {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-three-lines .mdc-list-item__secondary-text {
  white-space: normal;
  line-height: 20px;
}
.mdc-list-item--with-overline .mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: auto;
}

.mdc-list-item--with-leading-radio.mdc-list-item,
.mdc-list-item--with-leading-checkbox.mdc-list-item,
.mdc-list-item--with-leading-icon.mdc-list-item,
.mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
[dir=rtl] .mdc-list-item--with-leading-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-checkbox.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-icon.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  display: block;
  margin-top: 0;
  line-height: normal;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-trailing-icon.mdc-list-item, [dir=rtl] .mdc-list-item--with-trailing-icon.mdc-list-item {
  padding-left: 0;
  padding-right: 0;
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 16px;
}

.mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  -webkit-user-select: none;
  user-select: none;
  margin-left: 28px;
  margin-right: 16px;
}
[dir=rtl] .mdc-list-item--with-trailing-meta .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 28px;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end {
  display: block;
  line-height: normal;
  align-self: flex-start;
  margin-top: 0;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end::before, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-leading-radio .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 8px;
  margin-right: 24px;
}
[dir=rtl] .mdc-list-item--with-leading-radio .mdc-list-item__start,
[dir=rtl] .mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 24px;
  margin-right: 8px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-item--with-trailing-radio.mdc-list-item,
.mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-left: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, [dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-right: 0;
}
.mdc-list-item--with-trailing-radio .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 24px;
  margin-right: 8px;
}
[dir=rtl] .mdc-list-item--with-trailing-radio .mdc-list-item__end,
[dir=rtl] .mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 8px;
  margin-right: 24px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-three-lines .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-group__subheader {
  margin: 0.75rem 16px;
}

.mdc-list-item--disabled .mdc-list-item__start,
.mdc-list-item--disabled .mdc-list-item__content,
.mdc-list-item--disabled .mdc-list-item__end {
  opacity: 1;
}
.mdc-list-item--disabled .mdc-list-item__primary-text,
.mdc-list-item--disabled .mdc-list-item__secondary-text {
  opacity: var(--mat-list-list-item-disabled-label-text-opacity, 0.3);
}
.mdc-list-item--disabled.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--mat-list-list-item-disabled-leading-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-leading-icon-opacity, 0.38);
}
.mdc-list-item--disabled.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-disabled-trailing-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-trailing-icon-opacity, 0.38);
}

.mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing, [dir=rtl] .mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing {
  padding-left: 0;
  padding-right: 0;
}

.mdc-list-item.mdc-list-item--disabled .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-disabled-label-text-color, var(--mat-sys-on-surface));
}

.mdc-list-item:hover::before {
  background-color: var(--mat-list-list-item-hover-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}

.mdc-list-item.mdc-list-item--disabled::before {
  background-color: var(--mat-list-list-item-disabled-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-list-item:focus::before {
  background-color: var(--mat-list-list-item-focus-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-list-item--disabled .mdc-radio,
.mdc-list-item--disabled .mdc-checkbox {
  opacity: var(--mat-list-list-item-disabled-label-text-opacity, 0.3);
}

.mdc-list-item--with-leading-avatar .mat-mdc-list-item-avatar {
  border-radius: var(--mat-list-list-item-leading-avatar-shape, var(--mat-sys-corner-full));
  background-color: var(--mat-list-list-item-leading-avatar-color, var(--mat-sys-primary-container));
}

.mat-mdc-list-item-icon {
  font-size: var(--mat-list-list-item-leading-icon-size, 24px);
}

@media (forced-colors: active) {
  a.mdc-list-item--activated::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  a.mdc-list-item--activated [dir=rtl]::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-list-base {
  display: block;
}
.mat-mdc-list-base .mdc-list-item__start,
.mat-mdc-list-base .mdc-list-item__end,
.mat-mdc-list-base .mdc-list-item__content {
  pointer-events: auto;
}

.mat-mdc-list-item,
.mat-mdc-list-option {
  width: 100%;
  box-sizing: border-box;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-list-item:not(.mat-mdc-list-item-interactive),
.mat-mdc-list-option:not(.mat-mdc-list-item-interactive) {
  cursor: default;
}
.mat-mdc-list-item .mat-divider-inset,
.mat-mdc-list-option .mat-divider-inset {
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
}
.mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
.mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-left: 72px;
}
[dir=rtl] .mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
[dir=rtl] .mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-right: 72px;
}

.mat-mdc-list-item-interactive::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  content: "";
  opacity: 0;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-list-item > .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-list-item:focus-visible > .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-line.mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: normal;
}
.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-unscoped-content.mdc-list-item__secondary-text {
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

mat-action-list button {
  background: none;
  color: inherit;
  border: none;
  font: inherit;
  outline: inherit;
  -webkit-tap-highlight-color: transparent;
  text-align: start;
}
mat-action-list button::-moz-focus-inner {
  border: 0;
}

.mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-inline-start: var(--mat-list-list-item-leading-icon-start-space, 16px);
  margin-inline-end: var(--mat-list-list-item-leading-icon-end-space, 16px);
}

.mat-mdc-nav-list .mat-mdc-list-item {
  border-radius: var(--mat-list-active-indicator-shape, var(--mat-sys-corner-full));
  --mat-focus-indicator-border-radius: var(--mat-list-active-indicator-shape, var(--mat-sys-corner-full));
}
.mat-mdc-nav-list .mat-mdc-list-item.mdc-list-item--activated {
  background-color: var(--mat-list-active-indicator-color, var(--mat-sys-secondary-container));
}
`,J2=["unscopedContent"];var eB=[[["","matListItemTitle",""]],[["","matListItemLine",""]],"*",[["mat-divider"]],[["","matListItemAvatar",""],["","matListItemIcon",""]]],tB=["[matListItemTitle]","[matListItemLine]","*","mat-divider","[matListItemAvatar],[matListItemIcon]"];function nB(t,n){t&1&&se(0,4)}function iB(t,n){if(t&1&&(f(0,"div",11),M(1,"input",12),f(2,"div",13),Wt(),f(3,"svg",14),M(4,"path",15),h(),$i(),M(5,"div",16),h()()),t&2){let e=S();z("mdc-checkbox--disabled",e.disabled),p(),D("checked",e.selected)("disabled",e.disabled)}}function rB(t,n){if(t&1&&(f(0,"div",17),M(1,"input",18),f(2,"div",19),M(3,"div",20)(4,"div",21),h()()),t&2){let e=S();z("mdc-radio--disabled",e.disabled),p(),D("checked",e.selected)("disabled",e.disabled)}}function oB(t,n){}function sB(t,n){if(t&1&&(f(0,"span",4),Be(1,oB,0,0,"ng-template",6),h()),t&2){S();let e=Ve(3);p(),D("ngTemplateOutlet",e)}}function aB(t,n){}function cB(t,n){if(t&1&&(f(0,"span",5),Be(1,aB,0,0,"ng-template",6),h()),t&2){S();let e=Ve(5);p(),D("ngTemplateOutlet",e)}}function lB(t,n){}function dB(t,n){if(t&1&&Be(0,lB,0,0,"ng-template",6),t&2){S();let e=Ve(1);D("ngTemplateOutlet",e)}}function uB(t,n){}function mB(t,n){if(t&1&&(f(0,"span",9),Be(1,uB,0,0,"ng-template",6),h()),t&2){S();let e=Ve(3);p(),D("ngTemplateOutlet",e)}}function fB(t,n){}function hB(t,n){if(t&1&&(f(0,"span",9),Be(1,fB,0,0,"ng-template",6),h()),t&2){S();let e=Ve(5);p(),D("ngTemplateOutlet",e)}}function pB(t,n){}function gB(t,n){if(t&1&&Be(0,pB,0,0,"ng-template",6),t&2){S();let e=Ve(1);D("ngTemplateOutlet",e)}}var XI=new y("ListOption"),_B=(()=>{class t{_elementRef=d(P);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","matListItemTitle",""]],hostAttrs:[1,"mat-mdc-list-item-title","mdc-list-item__primary-text"]})}return t})(),vB=(()=>{class t{_elementRef=d(P);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","matListItemLine",""]],hostAttrs:[1,"mat-mdc-list-item-line","mdc-list-item__secondary-text"]})}return t})();var KI=(()=>{class t{_listOption=d(XI,{optional:!0});constructor(){}_isAlignedAtStart(){return!this._listOption||this._listOption?._getTogglePosition()==="after"}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,hostVars:4,hostBindings:function(i,r){i&2&&z("mdc-list-item__start",r._isAlignedAtStart())("mdc-list-item__end",!r._isAlignedAtStart())}})}return t})(),bB=(()=>{class t extends KI{static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["","matListItemAvatar",""]],hostAttrs:[1,"mat-mdc-list-item-avatar"],features:[_e]})}return t})(),yB=(()=>{class t extends KI{static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["","matListItemIcon",""]],hostAttrs:[1,"mat-mdc-list-item-icon"],features:[_e]})}return t})(),CB=new y("MAT_LIST_CONFIG"),hy=(()=>{class t{_isNonInteractive=!0;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=At(e)}_disableRipple=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(At(e))}_disabled=N(!1);_defaultOptions=d(CB,{optional:!0});static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,hostVars:1,hostBindings:function(i,r){i&2&&Y("aria-disabled",r.disabled)},inputs:{disableRipple:"disableRipple",disabled:"disabled"}})}return t})(),QI=(()=>{class t{_elementRef=d(P);_ngZone=d(j);_listBase=d(hy,{optional:!0});_platform=d(Ce);_hostElement;_isButtonElement;_noopAnimations=ke();_avatars;_icons;set lines(e){this._explicitLines=jn(e,null),this._updateItemLines(!1)}_explicitLines=null;get disableRipple(){return this.disabled||this._disableRipple||this._noopAnimations||!!this._listBase?.disableRipple}set disableRipple(e){this._disableRipple=At(e)}_disableRipple=!1;get disabled(){return this._disabled()||!!this._listBase?.disabled}set disabled(e){this._disabled.set(At(e))}_disabled=N(!1);_subscriptions=new ve;_rippleRenderer=null;_hasUnscopedTextContent=!1;rippleConfig;get rippleDisabled(){return this.disableRipple||!!this.rippleConfig.disabled}constructor(){d(qe).load(Tn);let e=d(Zl,{optional:!0});this.rippleConfig=e||{},this._hostElement=this._elementRef.nativeElement,this._isButtonElement=this._hostElement.nodeName.toLowerCase()==="button",this._listBase&&!this._listBase._isNonInteractive&&this._initInteractiveListItem(),this._isButtonElement&&!this._hostElement.hasAttribute("type")&&this._hostElement.setAttribute("type","button")}ngAfterViewInit(){this._monitorProjectedLinesAndTitle(),this._updateItemLines(!0)}ngOnDestroy(){this._subscriptions.unsubscribe(),this._rippleRenderer!==null&&this._rippleRenderer._removeTriggerEvents()}_hasIconOrAvatar(){return!!(this._avatars.length||this._icons.length)}_initInteractiveListItem(){this._hostElement.classList.add("mat-mdc-list-item-interactive"),this._rippleRenderer=new xs(this,this._ngZone,this._hostElement,this._platform,d(X)),this._rippleRenderer.setupTriggerEvents(this._hostElement)}_monitorProjectedLinesAndTitle(){this._ngZone.runOutsideAngular(()=>{this._subscriptions.add(fn(this._lines.changes,this._titles.changes).subscribe(()=>this._updateItemLines(!1)))})}_updateItemLines(e){if(!this._lines||!this._titles||!this._unscopedContent)return;e&&this._checkDomForUnscopedTextContent();let i=this._explicitLines??this._inferLinesFromContent(),r=this._unscopedContent.nativeElement;if(this._hostElement.classList.toggle("mat-mdc-list-item-single-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-one-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-two-lines",i===2),this._hostElement.classList.toggle("mdc-list-item--with-three-lines",i===3),this._hasUnscopedTextContent){let o=this._titles.length===0&&i===1;r.classList.toggle("mdc-list-item__primary-text",o),r.classList.toggle("mdc-list-item__secondary-text",!o)}else r.classList.remove("mdc-list-item__primary-text"),r.classList.remove("mdc-list-item__secondary-text")}_inferLinesFromContent(){let e=this._titles.length+this._lines.length;return this._hasUnscopedTextContent&&(e+=1),e}_checkDomForUnscopedTextContent(){this._hasUnscopedTextContent=Array.from(this._unscopedContent.nativeElement.childNodes).filter(e=>e.nodeType!==e.COMMENT_NODE).some(e=>!!(e.textContent&&e.textContent.trim()))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,contentQueries:function(i,r,o){if(i&1&&yt(o,bB,4)(o,yB,4),i&2){let s;U(s=$())&&(r._avatars=s),U(s=$())&&(r._icons=s)}},hostVars:4,hostBindings:function(i,r){i&2&&(Y("aria-disabled",r.disabled)("disabled",r._isButtonElement&&r.disabled||null),z("mdc-list-item--disabled",r.disabled))},inputs:{lines:"lines",disableRipple:"disableRipple",disabled:"disabled"}})}return t})();var JI=new y("SelectionList"),md=(()=>{class t extends QI{_selectionList=d(JI);_changeDetectorRef=d(ue);_lines;_titles;_unscopedContent;selectedChange=new H;togglePosition="after";get checkboxPosition(){return this.togglePosition}set checkboxPosition(e){this.togglePosition=e}get color(){return this._color||this._selectionList.color}set color(e){this._color=e}_color;get value(){return this._value}set value(e){this.selected&&e!==this.value&&this._inputsInitialized&&(this.selected=!1),this._value=e}_value;get selected(){return this._selectionList.selectedOptions.isSelected(this)}set selected(e){let i=At(e);i!==this._selected&&(this._setSelected(i),(i||this._selectionList.multiple)&&this._selectionList._reportValueChange())}_selected=!1;_inputsInitialized=!1;ngOnInit(){let e=this._selectionList;e._value&&e._value.some(r=>e.compareWith(this._value,r))&&this._setSelected(!0);let i=this._selected;Promise.resolve().then(()=>{(this._selected||i)&&(this.selected=!0,this._changeDetectorRef.markForCheck())}),this._inputsInitialized=!0}ngOnDestroy(){super.ngOnDestroy(),this.selected&&Promise.resolve().then(()=>{this.selected=!1})}toggle(){this.selected=!this.selected}focus(){this._hostElement.focus()}getLabel(){return(this._titles?.get(0)?._elementRef.nativeElement||this._unscopedContent?.nativeElement)?.textContent||""}_hasCheckboxAt(e){return this._selectionList.multiple&&this._getTogglePosition()===e}_hasRadioAt(e){return!this._selectionList.multiple&&this._getTogglePosition()===e&&!this._selectionList.hideSingleSelectionIndicator}_hasIconsOrAvatarsAt(e){return this._hasProjected("icons",e)||this._hasProjected("avatars",e)}_hasProjected(e,i){return this._getTogglePosition()!==i&&(e==="avatars"?this._avatars.length!==0:this._icons.length!==0)}_handleBlur(){this._selectionList._onTouched()}_getTogglePosition(){return this.togglePosition||"after"}_setSelected(e){return e===this._selected?!1:(this._selected=e,e?this._selectionList.selectedOptions.select(this):this._selectionList.selectedOptions.deselect(this),this.selectedChange.emit(e),this._changeDetectorRef.markForCheck(),!0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_toggleOnInteraction(){this.disabled||(this._selectionList.multiple?(this.selected=!this.selected,this._selectionList._emitChangeEvent([this])):this.selected||(this.selected=!0,this._selectionList._emitChangeEvent([this])))}_setTabindex(e){this._hostElement.setAttribute("tabindex",e+"")}_hasBothLeadingAndTrailing(){let e=this._hasProjected("avatars","before")||this._hasProjected("icons","before")||this._hasCheckboxAt("before")||this._hasRadioAt("before"),i=this._hasProjected("icons","after")||this._hasProjected("avatars","after")||this._hasCheckboxAt("after")||this._hasRadioAt("after");return e&&i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275cmp=E({type:t,selectors:[["mat-list-option"]],contentQueries:function(i,r,o){if(i&1&&yt(o,vB,5)(o,_B,5),i&2){let s;U(s=$())&&(r._lines=s),U(s=$())&&(r._titles=s)}},viewQuery:function(i,r){if(i&1&&je(J2,5),i&2){let o;U(o=$())&&(r._unscopedContent=o.first)}},hostAttrs:["role","option",1,"mat-mdc-list-item","mat-mdc-list-option","mdc-list-item"],hostVars:27,hostBindings:function(i,r){i&1&&R("blur",function(){return r._handleBlur()})("click",function(){return r._toggleOnInteraction()}),i&2&&(Y("aria-selected",r.selected),z("mdc-list-item--selected",r.selected&&!r._selectionList.multiple&&r._selectionList.hideSingleSelectionIndicator)("mdc-list-item--with-leading-avatar",r._hasProjected("avatars","before"))("mdc-list-item--with-leading-icon",r._hasProjected("icons","before"))("mdc-list-item--with-trailing-icon",r._hasProjected("icons","after"))("mat-mdc-list-option-with-trailing-avatar",r._hasProjected("avatars","after"))("mdc-list-item--with-leading-checkbox",r._hasCheckboxAt("before"))("mdc-list-item--with-trailing-checkbox",r._hasCheckboxAt("after"))("mdc-list-item--with-leading-radio",r._hasRadioAt("before"))("mdc-list-item--with-trailing-radio",r._hasRadioAt("after"))("mat-mdc-list-item-both-leading-and-trailing",r._hasBothLeadingAndTrailing())("mat-accent",r.color!=="primary"&&r.color!=="warn")("mat-warn",r.color==="warn")("_mat-animation-noopable",r._noopAnimations))},inputs:{togglePosition:"togglePosition",checkboxPosition:"checkboxPosition",color:"color",value:"value",selected:"selected"},outputs:{selectedChange:"selectedChange"},exportAs:["matListOption"],features:[Re([{provide:QI,useExisting:t},{provide:XI,useExisting:t}]),_e],ngContentSelectors:tB,decls:20,vars:4,consts:[["icons",""],["checkbox",""],["radio",""],["unscopedContent",""],[1,"mdc-list-item__start","mat-mdc-list-option-checkbox-before"],[1,"mdc-list-item__start","mat-mdc-list-option-radio-before"],[3,"ngTemplateOutlet"],[1,"mdc-list-item__content"],[1,"mat-mdc-list-item-unscoped-content",3,"cdkObserveContent"],[1,"mdc-list-item__end"],[1,"mat-focus-indicator"],[1,"mdc-checkbox"],["type","checkbox",1,"mdc-checkbox__native-control",3,"checked","disabled"],[1,"mdc-checkbox__background"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],[1,"mdc-radio"],["type","radio",1,"mdc-radio__native-control",3,"checked","disabled"],[1,"mdc-radio__background"],[1,"mdc-radio__outer-circle"],[1,"mdc-radio__inner-circle"]],template:function(i,r){i&1&&(Se(eB),Be(0,nB,1,0,"ng-template",null,0,Kn)(2,iB,6,4,"ng-template",null,1,Kn)(4,rB,5,4,"ng-template",null,2,Kn),F(6,sB,2,1,"span",4)(7,cB,2,1,"span",5),F(8,dB,1,1,null,6),f(9,"span",7),se(10),se(11,1),f(12,"span",8,3),R("cdkObserveContent",function(){return r._updateItemLines(!0)}),se(14,2),h()(),F(15,mB,2,1,"span",9)(16,hB,2,1,"span",9),F(17,gB,1,1,null,6),se(18,3),M(19,"div",10)),i&2&&(p(6),L(r._hasCheckboxAt("before")?6:r._hasRadioAt("before")?7:-1),p(2),L(r._hasIconsOrAvatarsAt("before")?8:-1),p(7),L(r._hasCheckboxAt("after")?15:r._hasRadioAt("after")?16:-1),p(2),L(r._hasIconsOrAvatarsAt("after")?17:-1))},dependencies:[Xi,ES],styles:[`.mat-mdc-list-option-with-trailing-avatar.mdc-list-item, [dir=rtl] .mat-mdc-list-option-with-trailing-avatar.mdc-list-item {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-list-option-with-trailing-avatar .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 16px;
  width: 40px;
  height: 40px;
}
.mat-mdc-list-option-with-trailing-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mat-mdc-list-option-with-trailing-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}
.mat-mdc-list-option-with-trailing-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}
.mat-mdc-list-option-with-trailing-avatar .mdc-list-item__end {
  border-radius: 50%;
}

.mat-mdc-list-option .mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}
.mat-mdc-list-option .mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-checkbox-state-layer-size, 40px);
  height: var(--mat-checkbox-state-layer-size, 40px);
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}
.mat-mdc-list-option .mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-list-option .mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--mat-checkbox-unselected-icon-color, var(--mat-sys-on-surface-variant));
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}
.mat-mdc-list-option .mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
}
.mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mat-mdc-list-option .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-hover-icon-color, var(--mat-sys-on-surface));
  background-color: transparent;
}
.mat-mdc-list-option .mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
}
.mat-mdc-list-option .mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-focus-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-list-option .mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
}
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
.mat-mdc-list-option .mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}
.mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}
.mat-mdc-list-option .mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}
.mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}
.mat-mdc-list-option .mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-list-option .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}
.mat-mdc-list-option .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mat-mdc-list-option .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}
.mat-mdc-list-option .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-list-option .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}
@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-list-option .mdc-radio {
  display: inline-block;
  position: relative;
  flex: 0 0 auto;
  box-sizing: content-box;
  width: 20px;
  height: 20px;
  cursor: pointer;
  will-change: opacity, transform, border-color, color;
  padding: calc((var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-list-option .mdc-radio__background {
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  width: 20px;
  height: 20px;
}
.mat-mdc-list-option .mdc-radio__background::before {
  position: absolute;
  transform: scale(0, 0);
  border-radius: 50%;
  opacity: 0;
  pointer-events: none;
  content: "";
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
  top: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
  left: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-list-option .mdc-radio__outer-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  border-width: 2px;
  border-style: solid;
  border-radius: 50%;
  transition: border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-list-option .mdc-radio__inner-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  transform: scale(0);
  border-radius: 50%;
  transition: transform 90ms cubic-bezier(0.4, 0, 0.6, 1), background-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-radio__inner-circle {
    background-color: CanvasText !important;
  }
}
.mat-mdc-list-option .mdc-radio__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  top: 0;
  right: 0;
  left: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
}
.mat-mdc-list-option .mdc-radio__native-control:checked + .mdc-radio__background, .mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background {
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle, .mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle, .mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option .mdc-radio__native-control:disabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-unselected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background {
  cursor: default;
}
.mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface, currentColor));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-list-option .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-list-option .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary));
}
.mat-mdc-list-option .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-list-option .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  transform: scale(0.5);
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option._mat-animation-noopable .mdc-radio__background::before,
.mat-mdc-list-option._mat-animation-noopable .mdc-radio__outer-circle,
.mat-mdc-list-option._mat-animation-noopable .mdc-radio__inner-circle {
  transition: none !important;
}
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark, .mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-list-option .mdc-checkbox__native-control, .mat-mdc-list-option .mdc-radio__native-control {
  display: none;
}

@media (forced-colors: active) {
  .mat-mdc-list-option.mdc-list-item--selected::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  .mat-mdc-list-option.mdc-list-item--selected [dir=rtl]::after {
    right: auto;
    left: 16px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var xB={provide:ki,useExisting:Rt(()=>fd),multi:!0},py=class{source;options;constructor(n,e){this.source=n,this.options=e}},fd=(()=>{class t extends hy{_element=d(P);_ngZone=d(j);_renderer=d(we);_initialized=!1;_keyManager;_listenerCleanups;_destroyed=new I;_isDestroyed=!1;_onChange=e=>{};_items;selectionChange=new H;color="accent";compareWith=(e,i)=>e===i;get multiple(){return this._multiple}set multiple(e){let i=At(e);i!==this._multiple&&(this._multiple=i,this.selectedOptions=new po(this._multiple,this.selectedOptions.selected))}_multiple=!0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=At(e)}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;selectedOptions=new po(this._multiple);_value=null;_onTouched=()=>{};_changeDetectorRef=d(ue);constructor(){super(),this._isNonInteractive=!1}ngAfterViewInit(){this._initialized=!0,this._setupRovingTabindex(),this._ngZone.runOutsideAngular(()=>{this._listenerCleanups=[this._renderer.listen(this._element.nativeElement,"focusin",this._handleFocusin),this._renderer.listen(this._element.nativeElement,"focusout",this._handleFocusout)]}),this._value&&this._setOptionsFromValues(this._value),this._watchForSelectionChange()}ngOnChanges(e){let i=e.disabled,r=e.disableRipple,o=e.hideSingleSelectionIndicator;(r&&!r.firstChange||i&&!i.firstChange||o&&!o.firstChange)&&this._markOptionsForCheck()}ngOnDestroy(){this._keyManager?.destroy(),this._listenerCleanups?.forEach(e=>e()),this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0}focus(e){this._element.nativeElement.focus(e)}selectAll(){return this._setAllOptionsSelected(!0)}deselectAll(){return this._setAllOptionsSelected(!1)}_reportValueChange(){if(this.options&&!this._isDestroyed){let e=this._getSelectedOptionValues();this._onChange(e),this._value=e}}_emitChangeEvent(e){this.selectionChange.emit(new py(this,e))}writeValue(e){this._value=e,this.options&&this._setOptionsFromValues(e||[])}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this._markOptionsForCheck()}get disabled(){return this._selectionListDisabled()}set disabled(e){this._selectionListDisabled.set(At(e)),this._selectionListDisabled()&&this._keyManager?.setActiveItem(-1)}_selectionListDisabled=N(!1);registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}_watchForSelectionChange(){this.selectedOptions.changed.pipe(pe(this._destroyed)).subscribe(e=>{for(let i of e.added)i.selected=!0;for(let i of e.removed)i.selected=!1;this._containsFocus()||this._resetActiveOption()})}_setOptionsFromValues(e){this.options.forEach(i=>i._setSelected(!1)),e.forEach(i=>{let r=this.options.find(o=>o.selected?!1:this.compareWith(o.value,i));r&&r._setSelected(!0)})}_getSelectedOptionValues(){return this.options.filter(e=>e.selected).map(e=>e.value)}_markOptionsForCheck(){this.options&&this.options.forEach(e=>e._markForCheck())}_setAllOptionsSelected(e,i){let r=[];return this.options.forEach(o=>{(!i||!o.disabled)&&o._setSelected(e)&&r.push(o)}),r.length&&this._reportValueChange(),r}get options(){return this._items}_handleKeydown(e){let i=this._keyManager.activeItem;if((e.keyCode===13||e.keyCode===32)&&!this._keyManager.isTyping()&&i&&!i.disabled)e.preventDefault(),i._toggleOnInteraction();else if(e.keyCode===65&&this.multiple&&!this._keyManager.isTyping()&&xt(e,"ctrlKey","metaKey")){let r=this.options.some(o=>!o.disabled&&!o.selected);e.preventDefault(),this._emitChangeEvent(this._setAllOptionsSelected(r,!0))}else this._keyManager.onKeydown(e)}_handleFocusout=()=>{setTimeout(()=>{this._containsFocus()||this._resetActiveOption()})};_handleFocusin=e=>{if(this.disabled)return;let i=this._items.toArray().findIndex(r=>r._elementRef.nativeElement.contains(e.target));i>-1?this._setActiveOption(i):this._resetActiveOption()};_setupRovingTabindex(){this._keyManager=new Er(this._items).withHomeAndEnd().withTypeAhead().withWrap().skipPredicate(()=>this.disabled),this._resetActiveOption(),this._keyManager.change.subscribe(e=>this._setActiveOption(e)),this._items.changes.pipe(pe(this._destroyed)).subscribe(()=>{let e=this._keyManager.activeItem;(!e||this._items.toArray().indexOf(e)===-1)&&this._resetActiveOption()})}_setActiveOption(e){this._items.forEach((i,r)=>i._setTabindex(r===e?0:-1)),this._keyManager.updateActiveItem(e)}_resetActiveOption(){if(this.disabled){this._setActiveOption(-1);return}let e=this._items.find(i=>i.selected&&!i.disabled)||this._items.first;this._setActiveOption(e?this._items.toArray().indexOf(e):-1)}_containsFocus(){let e=Dr();return e&&this._element.nativeElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-selection-list"]],contentQueries:function(i,r,o){if(i&1&&yt(o,md,5),i&2){let s;U(s=$())&&(r._items=s)}},hostAttrs:["role","listbox",1,"mat-mdc-selection-list","mat-mdc-list-base","mdc-list"],hostVars:1,hostBindings:function(i,r){i&1&&R("keydown",function(s){return r._handleKeydown(s)}),i&2&&Y("aria-multiselectable",r.multiple)},inputs:{color:"color",compareWith:"compareWith",multiple:"multiple",hideSingleSelectionIndicator:"hideSingleSelectionIndicator",disabled:"disabled"},outputs:{selectionChange:"selectionChange"},exportAs:["matSelectionList"],features:[Re([xB,{provide:hy,useExisting:t},{provide:JI,useExisting:t}]),_e,$e],ngContentSelectors:X2,decls:1,vars:0,template:function(i,r){i&1&&(Se(),se(0))},styles:[K2],encapsulation:2,changeDetection:0})}return t})();function wB(t,n){if(t&1&&(f(0,"mat-list-option",5),v(1),h()),t&2){let e=n.$implicit;D("value",e),p(),Z(e)}}function DB(t,n){if(t&1&&(f(0,"mat-list-option",5),v(1),h()),t&2){let e=n.$implicit;D("value",e),p(),Z(e)}}var hh=class t{shopService=d(Ji);dialogRef=d(td);data=d(Qb);selectedBrands=this.data.selectedBrands;selectedCategory=this.data.selectedCategory;applyFilters(){this.dialogRef.close({selectedBrands:this.selectedBrands,selectedCategory:this.selectedCategory})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-filters"]],decls:19,vars:4,consts:[[1,"text-3xl","text-center","pt-6","mb-3"],[1,"flex","p-4"],[1,"w-1/2"],[1,"font-semibold","text-xl","text-primary"],[3,"ngModelChange","ngModel","multiple"],[3,"value"],[1,"flex","justify-end","p-4"],["mat-flat-button","",3,"click"]],template:function(e,i){e&1&&(f(0,"div"),M(1,"h3",0)(2,"mat-divider"),f(3,"div",1)(4,"div",2)(5,"h4",3),v(6,"Brands"),h(),f(7,"mat-selection-list",4),yr("ngModelChange",function(o){return to(i.selectedBrands,o)||(i.selectedBrands=o),o}),st(8,wB,2,2,"mat-list-option",5,eo),h()(),f(10,"div",2)(11,"h4",3),v(12,"Category"),h(),f(13,"mat-selection-list",4),yr("ngModelChange",function(o){return to(i.selectedCategory,o)||(i.selectedCategory=o),o}),st(14,DB,2,2,"mat-list-option",5,eo),h()()(),f(16,"div",6)(17,"button",7),R("click",function(){return i.applyFilters()}),v(18,"Apply Filters"),h()()()),e&2&&(p(7),br("ngModel",i.selectedBrands),D("multiple",!0),p(),at(i.shopService.brands),p(5),br("ngModel",i.selectedCategory),D("multiple",!0),p(),at(i.shopService.category))},dependencies:[Ha,fd,md,pt,vo,ri,Ts],encapsulation:2})};var EB=["mat-menu-item",""],SB=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],IB=["mat-icon, [matMenuItemIcon]","*"];function kB(t,n){t&1&&(Wt(),f(0,"svg",2),M(1,"polygon",3),h())}var MB=["*"];function TB(t,n){if(t&1){let e=dt();Ne(0,"div",0),ua("click",function(){Me(e);let r=S();return Te(r.closed.emit("click"))})("animationstart",function(r){Me(e);let o=S();return Te(o._onAnimationStart(r.animationName))})("animationend",function(r){Me(e);let o=S();return Te(o._onAnimationDone(r.animationName))})("animationcancel",function(r){Me(e);let o=S();return Te(o._onAnimationDone(r.animationName))}),Ne(1,"div",1),se(2),Ge()()}if(t&2){let e=S();It(e._classList),z("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),ft("id",e.panelId),Y("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var _y=new y("MAT_MENU_PANEL"),hd=(()=>{class t{_elementRef=d(P);_document=d(K);_focusMonitor=d(Mn);_parentMenu=d(_y,{optional:!0});_changeDetectorRef=d(ue);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new I;_focused=new I;_highlighted=!1;_triggersSubmenu=!1;constructor(){d(qe).load(Tn),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&R("click",function(s){return r._checkDisabled(s)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(Y("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),z("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",V],disableRipple:[2,"disableRipple","disableRipple",V]},exportAs:["matMenuItem"],attrs:EB,ngContentSelectors:IB,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(Se(SB),se(0),f(1,"span",0),se(2,1),h(),M(3,"div",1),F(4,kB,2,0,":svg:svg",2)),i&2&&(p(3),D("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),p(),L(r._triggersSubmenu?4:-1))},dependencies:[tr],encapsulation:2,changeDetection:0})}return t})();var AB=new y("MatMenuContent");var RB=new y("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),gy="_mat-menu-enter",ph="_mat-menu-exit",bo=(()=>{class t{_elementRef=d(P);_changeDetectorRef=d(ue);_injector=d(X);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=ke();_allItems;_directDescendantItems=new Dn;_classList={};_panelAnimationState="void";_animationDone=new I;_isAnimating=N(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;set panelClass(e){let i=this._previousPanelClass,r=b({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass;get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new H;close=this.closed;panelId=d(Ie).getId("mat-menu-panel-");constructor(){let e=d(RB);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new Er(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe(Je(this._directDescendantItems),_t(e=>fn(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe(Je(this._directDescendantItems),_t(i=>fn(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:xt(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=bt(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=J(b({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===ph;(i||e===gy)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===gy||e===ph)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(ph),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?gy:ph)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe(Je(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&yt(o,AB,5)(o,hd,5)(o,hd,4),i&2){let s;U(s=$())&&(r.lazyContent=s.first),U(s=$())&&(r._allItems=s),U(s=$())&&(r.items=s)}},viewQuery:function(i,r){if(i&1&&je(mt,5),i&2){let o;U(o=$())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&Y("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",V],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:V(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[Re([{provide:_y,useExisting:t}])],ngContentSelectors:MB,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(Se(),da(0,TB,3,12,"ng-template"))},styles:[`mat-menu {
  display: none;
}

.mat-mdc-menu-content {
  margin: 0;
  padding: 8px 0;
  outline: 0;
}
.mat-mdc-menu-content,
.mat-mdc-menu-content .mat-mdc-menu-item .mat-mdc-menu-item-text {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  flex: 1;
  white-space: normal;
  font-family: var(--mat-menu-item-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-menu-item-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-menu-item-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-menu-item-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-menu-item-label-text-weight, var(--mat-sys-label-large-weight));
}

@keyframes _mat-menu-enter {
  from {
    opacity: 0;
    transform: scale(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-menu-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-menu-panel {
  min-width: 112px;
  max-width: 280px;
  overflow: auto;
  box-sizing: border-box;
  outline: 0;
  animation: _mat-menu-enter 120ms cubic-bezier(0, 0, 0.2, 1);
  border-radius: var(--mat-menu-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-menu-container-color, var(--mat-sys-surface-container));
  box-shadow: var(--mat-menu-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
  will-change: transform, opacity;
}
.mat-mdc-menu-panel.mat-menu-panel-exit-animation {
  animation: _mat-menu-exit 100ms 25ms linear forwards;
}
.mat-mdc-menu-panel.mat-menu-panel-animations-disabled {
  animation: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating {
  pointer-events: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating:has(.mat-mdc-menu-content:empty) {
  display: none;
}
@media (forced-colors: active) {
  .mat-mdc-menu-panel {
    outline: solid 1px;
  }
}
.mat-mdc-menu-panel .mat-divider {
  border-top-color: var(--mat-menu-divider-color, var(--mat-sys-surface-variant));
  margin-bottom: var(--mat-menu-divider-bottom-spacing, 8px);
  margin-top: var(--mat-menu-divider-top-spacing, 8px);
}

.mat-mdc-menu-item {
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  cursor: pointer;
  width: 100%;
  text-align: left;
  box-sizing: border-box;
  color: inherit;
  font-size: inherit;
  background: none;
  text-decoration: none;
  margin: 0;
  min-height: 48px;
  padding-left: var(--mat-menu-item-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-trailing-spacing, 12px);
  -webkit-user-select: none;
  user-select: none;
  cursor: pointer;
  outline: none;
  border: none;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-menu-item::-moz-focus-inner {
  border: 0;
}
[dir=rtl] .mat-mdc-menu-item {
  padding-left: var(--mat-menu-item-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-leading-spacing, 12px);
}
.mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-leading-spacing, 12px);
}
.mat-mdc-menu-item, .mat-mdc-menu-item:visited, .mat-mdc-menu-item:link {
  color: var(--mat-menu-item-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-menu-item .mat-icon-no-color,
.mat-mdc-menu-item .mat-mdc-menu-submenu-icon {
  color: var(--mat-menu-item-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-menu-item[disabled] {
  cursor: default;
  opacity: 0.38;
}
.mat-mdc-menu-item[disabled]::after {
  display: block;
  position: absolute;
  content: "";
  top: 0;
  left: 0;
  bottom: 0;
  right: 0;
}
.mat-mdc-menu-item:focus {
  outline: 0;
}
.mat-mdc-menu-item .mat-icon {
  flex-shrink: 0;
  margin-right: var(--mat-menu-item-spacing, 12px);
  height: var(--mat-menu-item-icon-size, 24px);
  width: var(--mat-menu-item-icon-size, 24px);
}
[dir=rtl] .mat-mdc-menu-item {
  text-align: right;
}
[dir=rtl] .mat-mdc-menu-item .mat-icon {
  margin-right: 0;
  margin-left: var(--mat-menu-item-spacing, 12px);
}
.mat-mdc-menu-item:not([disabled]):hover {
  background-color: var(--mat-menu-item-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-menu-item:not([disabled]).cdk-program-focused, .mat-mdc-menu-item:not([disabled]).cdk-keyboard-focused, .mat-mdc-menu-item:not([disabled]).mat-mdc-menu-item-highlighted {
  background-color: var(--mat-menu-item-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
@media (forced-colors: active) {
  .mat-mdc-menu-item {
    margin-top: 1px;
  }
}

.mat-mdc-menu-submenu-icon {
  width: var(--mat-menu-item-icon-size, 24px);
  height: 10px;
  fill: currentColor;
  padding-left: var(--mat-menu-item-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-submenu-icon {
  padding-right: var(--mat-menu-item-spacing, 12px);
  padding-left: 0;
}
[dir=rtl] .mat-mdc-menu-submenu-icon polygon {
  transform: scaleX(-1);
  transform-origin: center;
}
@media (forced-colors: active) {
  .mat-mdc-menu-submenu-icon {
    fill: CanvasText;
  }
}

.mat-mdc-menu-item .mat-mdc-menu-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
`],encapsulation:2,changeDetection:0})}return t})(),OB=new y("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(X);return()=>Mr(t)}});var Xa=new WeakMap,NB=(()=>{class t{_canHaveBackdrop;_element=d(P);_viewContainerRef=d(Nt);_menuItemInstance=d(hd,{optional:!0,self:!0});_dir=d(Jt,{optional:!0});_focusMonitor=d(Mn);_ngZone=d(j);_injector=d(X);_scrollStrategy=d(OB);_changeDetectorRef=d(ue);_animationsDisabled=ke();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=ve.EMPTY;_menuCloseSubscription=ve.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e&&(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=d(_y,{optional:!0});this._parentMaterialMenu=i instanceof bo?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&Xa.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=Xa.get(i);Xa.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),s=o.getConfig(),a=s.positionStrategy;this._setPosition(i,a),this._canHaveBackdrop?s.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:s.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof bo&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(pe(i.close)).subscribe(()=>{a.withLockedPosition(!1).reapplyLastPosition(),a.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof bo&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(Ke(1)).subscribe(()=>{i.detach(),Xa.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&Xa.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=Ei(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof bo&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new Di({positionStrategy:ks(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",s=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,s)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[s,a]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[c,l]=[s,a],[u,m]=[r,o],g=0;if(this._triggersSubmenu()){if(m=r=e.xPosition==="before"?"start":"end",o=u=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let _=this._parentMaterialMenu.items.first;this._parentInnerPadding=_?_._getHostElement().offsetTop:0}g=s==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(c=s==="top"?"bottom":"top",l=a==="top"?"bottom":"top");i.withPositions([{originX:r,originY:c,overlayX:u,overlayY:s,offsetY:g},{originX:o,originY:c,overlayX:m,overlayY:s,offsetY:g},{originX:r,originY:l,overlayX:u,overlayY:a,offsetY:-g},{originX:o,originY:l,overlayX:m,overlayY:a,offsetY:-g}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:Q(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(xe(s=>this._menuOpen&&s!==this._menuItemInstance)):Q();return fn(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new Vn(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return Xa.get(e)===this}_triggerIsAriaDisabled(){return V(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){xm()};static \u0275dir=T({type:t})}return t})(),gh=(()=>{class t extends NB{_cleanupTouchstart;_hoverSubscription=ve.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new H;onMenuOpen=this.menuOpened;menuClosed=new H;onMenuClose=this.menuClosed;constructor(){super(!0);let e=d(we);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{vs(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){_s(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&R("click",function(s){return r._handleClick(s)})("mousedown",function(s){return r._handleMousedown(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&Y("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu==null?null:r.menu.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[_e]})}return t})();var vy=class{_box;_destroyed=new I;_resizeSubject=new I;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new ce(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(xe(e=>e.some(i=>i.target===n)),cu({bufferSize:1,refCount:!0}),pe(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},ek=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(j);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new vy(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var PB=["notch"],FB=["matFormFieldNotchedOutline",""],LB=["*"],tk=["iconPrefixContainer"],nk=["textPrefixContainer"],ik=["iconSuffixContainer"],rk=["textSuffixContainer"],BB=["textField"],jB=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],VB=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function HB(t,n){t&1&&M(0,"span",21)}function zB(t,n){if(t&1&&(f(0,"label",20),se(1,1),F(2,HB,1,0,"span",21),h()),t&2){let e=S(2);D("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),Y("for",e._control.disableAutomaticLabeling?null:e._control.id),p(2),L(!e.hideRequiredMarker&&e._control.required?2:-1)}}function UB(t,n){if(t&1&&F(0,zB,3,5,"label",20),t&2){let e=S();L(e._hasFloatingLabel()?0:-1)}}function $B(t,n){t&1&&M(0,"div",7)}function GB(t,n){}function WB(t,n){if(t&1&&Be(0,GB,0,0,"ng-template",13),t&2){S(2);let e=Ve(1);D("ngTemplateOutlet",e)}}function qB(t,n){if(t&1&&(f(0,"div",9),F(1,WB,1,1,null,13),h()),t&2){let e=S();D("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),p(),L(e._forceDisplayInfixLabel()?-1:1)}}function YB(t,n){t&1&&(f(0,"div",10,2),se(2,2),h())}function ZB(t,n){t&1&&(f(0,"div",11,3),se(2,3),h())}function QB(t,n){}function XB(t,n){if(t&1&&Be(0,QB,0,0,"ng-template",13),t&2){S();let e=Ve(1);D("ngTemplateOutlet",e)}}function KB(t,n){t&1&&(f(0,"div",14,4),se(2,4),h())}function JB(t,n){t&1&&(f(0,"div",15,5),se(2,5),h())}function ej(t,n){t&1&&M(0,"div",16)}function tj(t,n){t&1&&(f(0,"div",18),se(1,6),h())}function nj(t,n){if(t&1&&(f(0,"mat-hint",22),v(1),h()),t&2){let e=S(2);D("id",e._hintLabelId),p(),Z(e.hintLabel)}}function ij(t,n){if(t&1&&(f(0,"div",19),F(1,nj,2,2,"mat-hint",22),se(2,7),M(3,"div",23),se(4,8),h()),t&2){let e=S();p(),L(e.hintLabel?1:-1)}}var Hn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["mat-label"]]})}return t})(),uk=new y("MatError"),gd=(()=>{class t{id=d(Ie).getId("mat-mdc-error-");constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["mat-error"],["","matError",""]],hostAttrs:[1,"mat-mdc-form-field-error","mat-mdc-form-field-bottom-align"],hostVars:1,hostBindings:function(i,r){i&2&&ft("id",r.id)},inputs:{id:"id"},features:[Re([{provide:uk,useExisting:t}])]})}return t})(),pd=(()=>{class t{align="start";id=d(Ie).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(ft("id",r.id),Y("align",null),z("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),rj=new y("MatPrefix");var oj=new y("MatSuffix");var mk=new y("FloatingLabelParent"),ok=(()=>{class t{_elementRef=d(P);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(ek);_ngZone=d(j);_parent=d(mk);_resizeSubscription=new ve;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return sj(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&z("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function sj(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var sk="mdc-line-ripple--active",_h="mdc-line-ripple--deactivating",ak=(()=>{class t{_elementRef=d(P);_cleanupTransitionEnd;constructor(){let e=d(j),i=d(we);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(_h),e.add(sk)}deactivate(){this._elementRef.nativeElement.classList.add(_h)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(_h);e.propertyName==="opacity"&&r&&i.remove(sk,_h)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),ck=(()=>{class t{_elementRef=d(P);_ngZone=d(j);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&je(PB,5),i&2){let o;U(o=$())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&z("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:FB,ngContentSelectors:LB,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Se(),zt(0,"div",1),Ne(1,"div",2,0),se(3),Ge(),zt(4,"div",3))},encapsulation:2,changeDetection:0})}return t})(),_d=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t})}return t})();var vd=new y("MatFormField"),aj=new y("MAT_FORM_FIELD_DEFAULT_OPTIONS"),lk="fill",cj="auto",dk="fixed",lj="translateY(-50%)",Rn=(()=>{class t{_elementRef=d(P);_changeDetectorRef=d(ue);_platform=d(Ce);_idGenerator=d(Ie);_ngZone=d(j);_defaults=d(aj,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=ol("iconPrefixContainer");_textPrefixContainerSignal=ol("textPrefixContainer");_iconSuffixContainerSignal=ol("iconSuffixContainer");_textSuffixContainerSignal=ol("textSuffixContainer");_prefixSuffixContainers=Ct(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=hD(Hn);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=At(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||cj}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||lk;this._appearanceSignal.set(i)}_appearanceSignal=N(lk);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||dk}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||dk}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new I;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=ke();constructor(){let e=this._defaults,i=d(Jt);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),hi(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=Ct(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(Je([void 0,void 0]),ee(()=>[i.errorState,i.userAriaDescribedBy]),au(),xe(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(pe(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),fn(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){_D({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=Ct(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,u=this._currentDirection==="rtl"?"-1":"1",m=`${s+a}px`,_=`calc(${u} * (${m} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,x=`var(--mat-mdc-form-field-label-transform, ${lj} translateX(${_}))`,O=s+a+c+l;return[x,O]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(Mm(o,r._labelChild,Hn,5),yt(o,_d,5)(o,rj,5)(o,oj,5)(o,uk,5)(o,pd,5)),i&2){Am();let s;U(s=$())&&(r._formFieldControl=s.first),U(s=$())&&(r._prefixChildren=s),U(s=$())&&(r._suffixChildren=s),U(s=$())&&(r._errorChildren=s),U(s=$())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(Tm(r._iconPrefixContainerSignal,tk,5)(r._textPrefixContainerSignal,nk,5)(r._iconSuffixContainerSignal,ik,5)(r._textSuffixContainerSignal,rk,5),je(BB,5)(tk,5)(nk,5)(ik,5)(rk,5)(ok,5)(ck,5)(ak,5)),i&2){Am(4);let o;U(o=$())&&(r._textField=o.first),U(o=$())&&(r._iconPrefixContainer=o.first),U(o=$())&&(r._textPrefixContainer=o.first),U(o=$())&&(r._iconSuffixContainer=o.first),U(o=$())&&(r._textSuffixContainer=o.first),U(o=$())&&(r._floatingLabel=o.first),U(o=$())&&(r._notchedOutline=o.first),U(o=$())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&z("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Re([{provide:vd,useExisting:t},{provide:mk,useExisting:t}])],ngContentSelectors:VB,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Se(jB),Be(0,UB,1,1,"ng-template",null,0,Kn),f(2,"div",6,1),R("click",function(s){return r._control.onContainerClick(s)}),F(4,$B,1,0,"div",7),f(5,"div",8),F(6,qB,2,2,"div",9),F(7,YB,3,0,"div",10),F(8,ZB,3,0,"div",11),f(9,"div",12),F(10,XB,1,1,null,13),se(11),h(),F(12,KB,3,0,"div",14),F(13,JB,3,0,"div",15),h(),F(14,ej,1,0,"div",16),h(),f(15,"div",17),F(16,tj,2,0,"div",18)(17,ij,5,1,"div",19),h()),i&2){let o;p(2),z("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),p(2),L(!r._hasOutline()&&!r._control.disabled?4:-1),p(2),L(r._hasOutline()?6:-1),p(),L(r._hasIconPrefix?7:-1),p(),L(r._hasTextPrefix?8:-1),p(2),L(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),p(2),L(r._hasTextSuffix?12:-1),p(),L(r._hasIconSuffix?13:-1),p(),L(r._hasOutline()?-1:14),p(),z("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();p(),L((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[ok,ck,Xi,ak,pd],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2,changeDetection:0})}return t})();var dj=["text"],uj=[[["mat-icon"]],"*"],mj=["mat-icon","*"];function fj(t,n){if(t&1&&M(0,"mat-pseudo-checkbox",1),t&2){let e=S();D("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function hj(t,n){if(t&1&&M(0,"mat-pseudo-checkbox",3),t&2){let e=S();D("disabled",e.disabled)}}function pj(t,n){if(t&1&&(f(0,"span",4),v(1),h()),t&2){let e=S();p(),De("(",e.group.label,")")}}var yy=new y("MAT_OPTION_PARENT_COMPONENT"),Cy=new y("MatOptgroup");var by=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},Ka=(()=>{class t{_element=d(P);_changeDetectorRef=d(ue);_parent=d(yy,{optional:!0});group=d(Cy,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(Ie).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=N(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new H;_text;_stateChanges=new I;constructor(){let e=d(qe);e.load(Tn),e.load(er),this._signalDisableRipple=!!this._parent&&Yi(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!xt(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new by(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&je(dj,7),i&2){let o;U(o=$())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&R("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(ft("id",r.id),Y("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),z("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",V]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:mj,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(Se(uj),F(0,fj,1,2,"mat-pseudo-checkbox",1),se(1),f(2,"span",2,0),se(4,1),h(),F(5,hj,1,1,"mat-pseudo-checkbox",3),F(6,pj,2,1,"span",4),M(7,"div",5)),i&2&&(L(r.multiple?0:-1),p(5),L(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),p(),L(r.group&&r.group._inert?6:-1),p(),D("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[YI,tr],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return t})();function fk(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let s=0;s<t+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function hk(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var yo=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ja=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var bd=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[jl,Rn,ye]})}return t})();var xy=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[uo,ZI,Ka,ye]})}return t})();var gj=["trigger"],_j=["panel"],vj=[[["mat-select-trigger"]],"*"],bj=["mat-select-trigger","*"];function yj(t,n){if(t&1&&(f(0,"span",4),v(1),h()),t&2){let e=S();p(),Z(e.placeholder)}}function Cj(t,n){t&1&&se(0)}function xj(t,n){if(t&1&&(f(0,"span",11),v(1),h()),t&2){let e=S(2);p(),Z(e.triggerValue)}}function wj(t,n){if(t&1&&(f(0,"span",5),F(1,Cj,1,0)(2,xj,2,1,"span",11),h()),t&2){let e=S();p(),L(e.customTrigger?1:2)}}function Dj(t,n){if(t&1){let e=dt();f(0,"div",12,1),R("keydown",function(r){Me(e);let o=S();return Te(o._handleKeydown(r))}),se(2,1),h()}if(t&2){let e=S();It(e.panelClass),z("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",(e._parentFormField==null?null:e._parentFormField.color)==="primary")("mat-accent",(e._parentFormField==null?null:e._parentFormField.color)==="accent")("mat-warn",(e._parentFormField==null?null:e._parentFormField.color)==="warn")("mat-undefined",!(e._parentFormField!=null&&e._parentFormField.color)),Y("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var Ej=new y("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(X);return()=>Mr(t)}}),Sj=new y("MAT_SELECT_CONFIG"),Ij=new y("MatSelectTrigger"),wy=class{source;value;constructor(n,e){this.source=n,this.value=e}},_k=(()=>{class t{_viewportRuler=d(Ir);_changeDetectorRef=d(ue);_elementRef=d(P);_dir=d(Jt,{optional:!0});_idGenerator=d(Ie);_renderer=d(we);_parentFormField=d(vd,{optional:!0});ngControl=d(xn,{self:!0,optional:!0});_liveAnnouncer=d(Vl);_defaultOptions=d(Sj,{optional:!0});_animationsDisabled=ke();_popoverLocation;_initialized=new I;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=fk(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=hk(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new wy(this,e)}_scrollStrategyFactory=d(Ej);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new I;_errorStateTracker;stateChanges=new I;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=N(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(Si.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=li(()=>{let e=this.options;return e?e.changes.pipe(Je(e),_t(()=>fn(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(_t(()=>this.optionSelectionChanges))});openedChange=new H;_openedStream=this.openedChange.pipe(xe(e=>e),ee(()=>{}));_closedStream=this.openedChange.pipe(xe(e=>!e),ee(()=>{}));selectionChange=new H;valueChange=new H;constructor(){let e=d(yo),i=d(Ms,{optional:!0}),r=d(Ar,{optional:!0}),o=d(new Jn("tabindex"),{optional:!0}),s=d(Jl,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Ja(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new po(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(pe(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(pe(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(Je(null),pe(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Ke(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let e=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=`${this.id}-panel`;this._trackedModal&&Bf(this._trackedModal,"aria-owns",i),Ob(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(!this._trackedModal)return;let e=`${this.id}-panel`;Bf(this._trackedModal,"aria-owns",e),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!xt(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let c=this.selected;c&&a!==c&&this._liveAnnouncer.announce(c.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!xt(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(c=>!c.disabled&&!c.selected);this.options.forEach(c=>{c.disabled||(a?c.select():c.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!xt(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof ja?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Wl(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=fn(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(pe(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),fn(...this.options.map(i=>i._stateChanges)).pipe(pe(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=en(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&yt(o,Ij,5)(o,Ka,5)(o,Cy,5),i&2){let s;U(s=$())&&(r.customTrigger=s.first),U(s=$())&&(r.options=s),U(s=$())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&je(gj,5)(_j,5)(Kf,5),i&2){let o;U(o=$())&&(r.trigger=o.first),U(o=$())&&(r.panel=o.first),U(o=$())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&R("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(Y("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),z("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",V],disableRipple:[2,"disableRipple","disableRipple",V],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:ut(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",V],placeholder:"placeholder",required:[2,"required","required",V],multiple:[2,"multiple","multiple",V],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",V],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",ut],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",V]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Re([{provide:_d,useExisting:t},{provide:yy,useExisting:t}]),$e],ngContentSelectors:bj,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(Se(vj),f(0,"div",2,0),R("click",function(){return r.open()}),f(3,"div",3),F(4,yj,2,1,"span",4)(5,wj,3,1,"span",5),h(),f(6,"div",6)(7,"div",7),Wt(),f(8,"svg",8),M(9,"path",9),h()()()(),Be(10,Dj,3,16,"ng-template",10),R("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=Ve(1);p(3),Y("id",r._valueId),p(),L(r.empty?4:5),p(6),D("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[ja,Kf],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-select-enabled-trigger-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-select-trigger-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-select-trigger-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-select-trigger-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-select-trigger-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-select-trigger-text-tracking, var(--mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--mat-select-invalid-arrow-color, var(--mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--mat-select-enabled-arrow-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--mat-select-focused-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--mat-select-disabled-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--mat-select-panel-background-color, var(--mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--mat-select-placeholder-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2,changeDetection:0})}return t})();var vk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[Tr,xy,ye,Ds,bd,xy]})}return t})();var kj=["tooltip"],Mj=20;var Tj=new y("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(X);return()=>Mr(t,{scrollThrottle:Mj})}}),Aj=new y("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var bk="tooltip-panel",Rj={passive:!0},Oj=8,Nj=8,Pj=24,Fj=200,yk=(()=>{class t{_elementRef=d(P);_ngZone=d(j);_platform=d(Ce);_ariaDescriber=d(jf);_focusMonitor=d(Mn);_dir=d(Jt);_injector=d(X);_viewContainerRef=d(Nt);_mediaMatcher=d(Na);_document=d(K);_renderer=d(we);_animationsDisabled=ke();_defaultOptions=d(Aj,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=Lj;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=At(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let i=At(e);this._disabled!==i&&(this._disabled=i,i?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=jn(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=jn(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let i=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(i)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new I;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=Oj}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(pe(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(i=>i()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,i){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let r=this._createOverlay(i);this._detach(),this._portal=this._portal||new wi(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=r.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(pe(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let i=this._tooltipInstance;i&&(i.isVisible()?i.hide(e):(i._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let s=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&s._origin instanceof P)return this._overlayRef;this._detach()}let i=this._injector.get(Es).getAncestorScrollContainers(this._elementRef),r=`${this._cssClassPrefix}-${bk}`,o=ks(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(i).withPopoverLocation("global");return o.positionChanges.pipe(pe(this._destroyed)).subscribe(s=>{this._updateCurrentPositionClass(s.connectionPair),this._tooltipInstance&&s.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=Ei(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,r]:r,scrollStrategy:this._injector.get(Tj)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(pe(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(pe(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(pe(this._destroyed)).subscribe(s=>{s.preventDefault(),s.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(pe(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let i=e.getConfig().positionStrategy,r=this._getOrigin(),o=this._getOverlayPosition();i.withPositions([this._addOffset(b(b({},r.main),o.main)),this._addOffset(b(b({},r.fallback),o.fallback))])}_addOffset(e){let i=Nj,r=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-i:e.originY==="bottom"?e.offsetY=i:e.originX==="start"?e.offsetX=r?-i:i:e.originX==="end"&&(e.offsetX=r?i:-i),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"||i=="below"?r={originX:"center",originY:i=="above"?"top":"bottom"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={originX:"start",originY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={originX:"end",originY:"center"});let{x:o,y:s}=this._invertPosition(r.originX,r.originY);return{main:r,fallback:{originX:o,originY:s}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"?r={overlayX:"center",overlayY:"bottom"}:i=="below"?r={overlayX:"center",overlayY:"top"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={overlayX:"end",overlayY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={overlayX:"start",overlayY:"center"});let{x:o,y:s}=this._invertPosition(r.overlayX,r.overlayY);return{main:r,fallback:{overlayX:o,overlayY:s}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),bt(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,i){return this.position==="above"||this.position==="below"?i==="top"?i="bottom":i==="bottom"&&(i="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:i}}_updateCurrentPositionClass(e){let{overlayY:i,originX:r,originY:o}=e,s;if(i==="center"?this._dir&&this._dir.value==="rtl"?s=r==="end"?"left":"right":s=r==="start"?"left":"right":s=i==="bottom"&&o==="top"?"above":"below",s!==this._currentPosition){let a=this._overlayRef;if(a){let c=`${this._cssClassPrefix}-${bk}-`;a.removePanelClass(c+this._currentPosition),a.addPanelClass(c+s)}this._currentPosition=s}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let i=e.targetTouches?.[0],r=i?{x:i.clientX,y:i.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,r)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let i;e.x!==void 0&&e.y!==void 0&&(i=e),this.show(void 0,i)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let i=e.relatedTarget;(!i||!this._overlayRef?.overlayElement.contains(i))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let i=this._document.elementFromPoint(e.clientX,e.clientY),r=this._elementRef.nativeElement;i!==r&&!r.contains(i)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,i){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,i,Rj))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let i=this._elementRef.nativeElement,r=i.style;(e==="on"||i.nodeName!=="INPUT"&&i.nodeName!=="TEXTAREA")&&(r.userSelect=r.msUserSelect=r.webkitUserSelect=r.MozUserSelect="none"),(e==="on"||!i.draggable)&&(r.webkitUserDrag="none"),r.touchAction="none",r.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||bt({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!xt(e):!0;static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(i,r){i&2&&z("mat-mdc-tooltip-disabled",r.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return t})(),Lj=(()=>{class t{_changeDetectorRef=d(ue);_elementRef=d(P);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=ke();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new I;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";constructor(){}show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>Pj&&e.width>=Fj}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let i=this._tooltip.nativeElement,r=this._showAnimation,o=this._hideAnimation;if(i.classList.remove(e?o:r),i.classList.add(e?r:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let s=getComputedStyle(i);(s.getPropertyValue("animation-duration")==="0s"||s.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(i.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-tooltip-component"]],viewQuery:function(i,r){if(i&1&&je(kj,7),i&2){let o;U(o=$())&&(r._tooltip=o.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,r){i&1&&R("mouseleave",function(s){return r._handleMouseLeave(s)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,r){i&1&&(Ne(0,"div",1,0),ua("animationend",function(s){return r._handleAnimationEnd(s)}),Ne(2,"div",2),v(3),Ge()()),i&2&&(It(r.tooltipClass),z("mdc-tooltip--multiline",r._isMultiline),p(3),Z(r.message))},styles:[`.mat-mdc-tooltip {
  position: relative;
  transform: scale(0);
  display: inline-flex;
}
.mat-mdc-tooltip::before {
  content: "";
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: -1;
  position: absolute;
}
.mat-mdc-tooltip-panel-below .mat-mdc-tooltip::before {
  top: -8px;
}
.mat-mdc-tooltip-panel-above .mat-mdc-tooltip::before {
  bottom: -8px;
}
.mat-mdc-tooltip-panel-right .mat-mdc-tooltip::before {
  left: -8px;
}
.mat-mdc-tooltip-panel-left .mat-mdc-tooltip::before {
  right: -8px;
}
.mat-mdc-tooltip._mat-animation-noopable {
  animation: none;
  transform: scale(1);
}

.mat-mdc-tooltip-surface {
  word-break: normal;
  overflow-wrap: anywhere;
  padding: 4px 8px;
  min-width: 40px;
  max-width: 200px;
  min-height: 24px;
  max-height: 40vh;
  box-sizing: border-box;
  overflow: hidden;
  text-align: center;
  will-change: transform, opacity;
  background-color: var(--mat-tooltip-container-color, var(--mat-sys-inverse-surface));
  color: var(--mat-tooltip-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-tooltip-container-shape, var(--mat-sys-corner-extra-small));
  font-family: var(--mat-tooltip-supporting-text-font, var(--mat-sys-body-small-font));
  font-size: var(--mat-tooltip-supporting-text-size, var(--mat-sys-body-small-size));
  font-weight: var(--mat-tooltip-supporting-text-weight, var(--mat-sys-body-small-weight));
  line-height: var(--mat-tooltip-supporting-text-line-height, var(--mat-sys-body-small-line-height));
  letter-spacing: var(--mat-tooltip-supporting-text-tracking, var(--mat-sys-body-small-tracking));
}
.mat-mdc-tooltip-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
.mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: left;
}
[dir=rtl] .mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: right;
}

.mat-mdc-tooltip-panel {
  line-height: normal;
}
.mat-mdc-tooltip-panel.mat-mdc-tooltip-panel-non-interactive {
  pointer-events: none;
}

@keyframes mat-mdc-tooltip-show {
  0% {
    opacity: 0;
    transform: scale(0.8);
  }
  100% {
    opacity: 1;
    transform: scale(1);
  }
}
@keyframes mat-mdc-tooltip-hide {
  0% {
    opacity: 1;
    transform: scale(1);
  }
  100% {
    opacity: 0;
    transform: scale(0.8);
  }
}
.mat-mdc-tooltip-show {
  animation: mat-mdc-tooltip-show 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}

.mat-mdc-tooltip-hide {
  animation: mat-mdc-tooltip-hide 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}
`],encapsulation:2,changeDetection:0})}return t})();var Ck=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[Hl,Tr,ye,Ds]})}return t})();function Bj(t,n){if(t&1&&(f(0,"mat-option",17),v(1),h()),t&2){let e=n.$implicit;D("value",e),p(),De(" ",e," ")}}function jj(t,n){if(t&1){let e=dt();f(0,"mat-form-field",14)(1,"mat-select",16,0),R("selectionChange",function(r){Me(e);let o=S(2);return Te(o._changePageSize(r.value))}),st(3,Bj,2,2,"mat-option",17,cs),h(),f(5,"div",18),R("click",function(){Me(e);let r=Ve(2);return Te(r.open())}),h()()}if(t&2){let e=S(2);D("appearance",e._formFieldAppearance)("color",e.color),p(),D("value",e.pageSize)("disabled",e.disabled),km("aria-labelledby",e._pageSizeLabelId),D("panelClass",e.selectConfig.panelClass||"")("disableOptionCentering",e.selectConfig.disableOptionCentering),p(2),at(e._displayedPageSizeOptions)}}function Vj(t,n){if(t&1&&(f(0,"div",15),v(1),h()),t&2){let e=S(2);p(),Z(e.pageSize)}}function Hj(t,n){if(t&1&&(f(0,"div",3)(1,"div",13),v(2),h(),F(3,jj,6,7,"mat-form-field",14),F(4,Vj,2,1,"div",15),h()),t&2){let e=S();p(),Y("id",e._pageSizeLabelId),p(),De(" ",e._intl.itemsPerPageLabel," "),p(),L(e._displayedPageSizeOptions.length>1?3:-1),p(),L(e._displayedPageSizeOptions.length<=1?4:-1)}}function zj(t,n){if(t&1){let e=dt();f(0,"button",19),R("click",function(){Me(e);let r=S();return Te(r._buttonClicked(0,r._previousButtonsDisabled()))}),Wt(),f(1,"svg",8),M(2,"path",20),h()()}if(t&2){let e=S();D("matTooltip",e._intl.firstPageLabel)("matTooltipDisabled",e._previousButtonsDisabled())("disabled",e._previousButtonsDisabled())("tabindex",e._previousButtonsDisabled()?-1:null),Y("aria-label",e._intl.firstPageLabel)}}function Uj(t,n){if(t&1){let e=dt();f(0,"button",21),R("click",function(){Me(e);let r=S();return Te(r._buttonClicked(r.getNumberOfPages()-1,r._nextButtonsDisabled()))}),Wt(),f(1,"svg",8),M(2,"path",22),h()()}if(t&2){let e=S();D("matTooltip",e._intl.lastPageLabel)("matTooltipDisabled",e._nextButtonsDisabled())("disabled",e._nextButtonsDisabled())("tabindex",e._nextButtonsDisabled()?-1:null),Y("aria-label",e._intl.lastPageLabel)}}var $j=(()=>{class t{changes=new I;itemsPerPageLabel="Items per page:";nextPageLabel="Next page";previousPageLabel="Previous page";firstPageLabel="First page";lastPageLabel="Last page";getRangeLabel=(e,i,r)=>{if(r==0||i==0)return`0 of ${r}`;r=Math.max(r,0);let o=e*i,s=o<r?Math.min(o+i,r):o+i;return`${o+1} \u2013 ${s} of ${r}`};static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Gj=50;var Wj=new y("MAT_PAGINATOR_DEFAULT_OPTIONS"),Dy=(()=>{class t{_intl=d($j);_changeDetectorRef=d(ue);_formFieldAppearance;_pageSizeLabelId=d(Ie).getId("mat-paginator-page-size-label-");_intlChanges;_isInitialized=!1;_initializedStream=new Ai(1);color;get pageIndex(){return this._pageIndex}set pageIndex(e){this._pageIndex=Math.max(e||0,0),this._changeDetectorRef.markForCheck()}_pageIndex=0;get length(){return this._length}set length(e){this._length=e||0,this._changeDetectorRef.markForCheck()}_length=0;get pageSize(){return this._pageSize}set pageSize(e){this._pageSize=Math.max(e||0,0),this._updateDisplayedPageSizeOptions()}_pageSize;get pageSizeOptions(){return this._pageSizeOptions}set pageSizeOptions(e){this._pageSizeOptions=(e||[]).map(i=>ut(i,0)),this._updateDisplayedPageSizeOptions()}_pageSizeOptions=[];hidePageSize=!1;showFirstLastButtons=!1;selectConfig={};disabled=!1;page=new H;_displayedPageSizeOptions;initialized=this._initializedStream;constructor(){let e=this._intl,i=d(Wj,{optional:!0});if(this._intlChanges=e.changes.subscribe(()=>this._changeDetectorRef.markForCheck()),i){let{pageSize:r,pageSizeOptions:o,hidePageSize:s,showFirstLastButtons:a}=i;r!=null&&(this._pageSize=r),o!=null&&(this._pageSizeOptions=o),s!=null&&(this.hidePageSize=s),a!=null&&(this.showFirstLastButtons=a)}this._formFieldAppearance=i?.formFieldAppearance||"outline"}ngOnInit(){this._isInitialized=!0,this._updateDisplayedPageSizeOptions(),this._initializedStream.next()}ngOnDestroy(){this._initializedStream.complete(),this._intlChanges.unsubscribe()}nextPage(){this.hasNextPage()&&this._navigate(this.pageIndex+1)}previousPage(){this.hasPreviousPage()&&this._navigate(this.pageIndex-1)}firstPage(){this.hasPreviousPage()&&this._navigate(0)}lastPage(){this.hasNextPage()&&this._navigate(this.getNumberOfPages()-1)}hasPreviousPage(){return this.pageIndex>=1&&this.pageSize!=0}hasNextPage(){let e=this.getNumberOfPages()-1;return this.pageIndex<e&&this.pageSize!=0}getNumberOfPages(){return this.pageSize?Math.ceil(this.length/this.pageSize):0}_changePageSize(e){let i=this.pageIndex*this.pageSize,r=this.pageIndex;this.pageIndex=Math.floor(i/e)||0,this.pageSize=e,this._emitPageEvent(r)}_nextButtonsDisabled(){return this.disabled||!this.hasNextPage()}_previousButtonsDisabled(){return this.disabled||!this.hasPreviousPage()}_updateDisplayedPageSizeOptions(){this._isInitialized&&(this.pageSize||(this._pageSize=this.pageSizeOptions.length!=0?this.pageSizeOptions[0]:Gj),this._displayedPageSizeOptions=this.pageSizeOptions.slice(),this._displayedPageSizeOptions.indexOf(this.pageSize)===-1&&this._displayedPageSizeOptions.push(this.pageSize),this._displayedPageSizeOptions.sort((e,i)=>e-i),this._changeDetectorRef.markForCheck())}_emitPageEvent(e){this.page.emit({previousPageIndex:e,pageIndex:this.pageIndex,pageSize:this.pageSize,length:this.length})}_navigate(e){let i=this.pageIndex;e!==i&&(this.pageIndex=e,this._emitPageEvent(i))}_buttonClicked(e,i){i||this._navigate(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-paginator"]],hostAttrs:["role","group",1,"mat-mdc-paginator"],inputs:{color:"color",pageIndex:[2,"pageIndex","pageIndex",ut],length:[2,"length","length",ut],pageSize:[2,"pageSize","pageSize",ut],pageSizeOptions:"pageSizeOptions",hidePageSize:[2,"hidePageSize","hidePageSize",V],showFirstLastButtons:[2,"showFirstLastButtons","showFirstLastButtons",V],selectConfig:"selectConfig",disabled:[2,"disabled","disabled",V]},outputs:{page:"page"},exportAs:["matPaginator"],decls:14,vars:14,consts:[["selectRef",""],[1,"mat-mdc-paginator-outer-container"],[1,"mat-mdc-paginator-container"],[1,"mat-mdc-paginator-page-size"],[1,"mat-mdc-paginator-range-actions"],["aria-atomic","true","aria-live","polite","role","status",1,"mat-mdc-paginator-range-label"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-first",3,"matTooltip","matTooltipDisabled","disabled","tabindex"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-previous",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["viewBox","0 0 24 24","focusable","false","aria-hidden","true",1,"mat-mdc-paginator-icon"],["d","M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-next",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-last",3,"matTooltip","matTooltipDisabled","disabled","tabindex"],["aria-hidden","true",1,"mat-mdc-paginator-page-size-label"],[1,"mat-mdc-paginator-page-size-select",3,"appearance","color"],[1,"mat-mdc-paginator-page-size-value"],["hideSingleSelectionIndicator","",3,"selectionChange","value","disabled","aria-labelledby","panelClass","disableOptionCentering"],[3,"value"],[1,"mat-mdc-paginator-touch-target",3,"click"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-first",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M18.41 16.59L13.82 12l4.59-4.59L17 6l-6 6 6 6zM6 6h2v12H6z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-last",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M5.59 7.41L10.18 12l-4.59 4.59L7 18l6-6-6-6zM16 6h2v12h-2z"]],template:function(i,r){i&1&&(f(0,"div",1)(1,"div",2),F(2,Hj,5,4,"div",3),f(3,"div",4)(4,"div",5),v(5),h(),F(6,zj,3,5,"button",6),f(7,"button",7),R("click",function(){return r._buttonClicked(r.pageIndex-1,r._previousButtonsDisabled())}),Wt(),f(8,"svg",8),M(9,"path",9),h()(),$i(),f(10,"button",10),R("click",function(){return r._buttonClicked(r.pageIndex+1,r._nextButtonsDisabled())}),Wt(),f(11,"svg",8),M(12,"path",11),h()(),F(13,Uj,3,5,"button",12),h()()()),i&2&&(p(2),L(r.hidePageSize?-1:2),p(3),De(" ",r._intl.getRangeLabel(r.pageIndex,r.pageSize,r.length)," "),p(),L(r.showFirstLastButtons?6:-1),p(),D("matTooltip",r._intl.previousPageLabel)("matTooltipDisabled",r._previousButtonsDisabled())("disabled",r._previousButtonsDisabled())("tabindex",r._previousButtonsDisabled()?-1:null),Y("aria-label",r._intl.previousPageLabel),p(3),D("matTooltip",r._intl.nextPageLabel)("matTooltipDisabled",r._nextButtonsDisabled())("disabled",r._nextButtonsDisabled())("tabindex",r._nextButtonsDisabled()?-1:null),Y("aria-label",r._intl.nextPageLabel),p(3),L(r.showFirstLastButtons?13:-1))},dependencies:[Rn,_k,Ka,ws,yk],styles:[`.mat-mdc-paginator {
  display: block;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-paginator-container-text-color, var(--mat-sys-on-surface));
  background-color: var(--mat-paginator-container-background-color, var(--mat-sys-surface));
  font-family: var(--mat-paginator-container-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-paginator-container-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-paginator-container-text-size, var(--mat-sys-body-small-size));
  font-weight: var(--mat-paginator-container-text-weight, var(--mat-sys-body-small-weight));
  letter-spacing: var(--mat-paginator-container-text-tracking, var(--mat-sys-body-small-tracking));
  --mat-form-field-container-height: var(--mat-paginator-form-field-container-height, 40px);
  --mat-form-field-container-vertical-padding: var(--mat-paginator-form-field-container-vertical-padding, 8px);
}
.mat-mdc-paginator .mat-mdc-select-value {
  font-size: var(--mat-paginator-select-trigger-text-size, var(--mat-sys-body-small-size));
}
.mat-mdc-paginator .mat-mdc-form-field-subscript-wrapper {
  display: none;
}
.mat-mdc-paginator .mat-mdc-select {
  line-height: 1.5;
}

.mat-mdc-paginator-outer-container {
  display: flex;
}

.mat-mdc-paginator-container {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  padding: 0 8px;
  flex-wrap: wrap;
  width: 100%;
  min-height: var(--mat-paginator-container-size, 56px);
}

.mat-mdc-paginator-page-size {
  display: flex;
  align-items: baseline;
  margin-right: 8px;
}
[dir=rtl] .mat-mdc-paginator-page-size {
  margin-right: 0;
  margin-left: 8px;
}

.mat-mdc-paginator-page-size-label {
  margin: 0 4px;
}

.mat-mdc-paginator-page-size-select {
  margin: 0 4px;
  width: var(--mat-paginator-page-size-select-width, 84px);
}

.mat-mdc-paginator-range-label {
  margin: 0 32px 0 24px;
}

.mat-mdc-paginator-range-actions {
  display: flex;
  align-items: center;
}

.mat-mdc-paginator-icon {
  display: inline-block;
  width: 28px;
  fill: var(--mat-paginator-enabled-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button[aria-disabled] .mat-mdc-paginator-icon {
  fill: var(--mat-paginator-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
[dir=rtl] .mat-mdc-paginator-icon {
  transform: rotate(180deg);
}

@media (forced-colors: active) {
  .mat-mdc-icon-button[aria-disabled] .mat-mdc-paginator-icon,
  .mat-mdc-paginator-icon {
    fill: currentColor;
  }
  .mat-mdc-paginator-range-actions .mat-mdc-icon-button {
    outline: solid 1px;
  }
  .mat-mdc-paginator-range-actions .mat-mdc-icon-button[aria-disabled] {
    color: GrayText;
  }
}
.mat-mdc-paginator-touch-target {
  display: var(--mat-paginator-touch-target-display, block);
  position: absolute;
  top: 50%;
  left: 50%;
  width: var(--mat-paginator-page-size-select-width, 84px);
  height: var(--mat-paginator-page-size-select-touch-target-height, 48px);
  background-color: transparent;
  transform: translate(-50%, -50%);
  cursor: pointer;
}
`],encapsulation:2,changeDetection:0})}return t})(),xk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[Hb,vk,Ck,Dy]})}return t})();var Yj=()=>[],Zj=(t,n)=>n.id;function Qj(t,n){if(t&1&&M(0,"app-product-item",9),t&2){let e=n.$implicit;D("product",e)}}function Xj(t,n){if(t&1&&(f(0,"mat-list-option",11),v(1),h()),t&2){let e=n.$implicit,i=S(2);D("value",e.value)("selected",i.shopParams.sort===e.value),p(),De(" ",e.name," ")}}function Kj(t,n){if(t&1){let e=dt();f(0,"div",1)(1,"div",2)(2,"div",3)(3,"button",4),R("click",function(){Me(e);let r=S();return Te(r.openFiltersDialog())}),f(4,"mat-icon"),v(5,"filter_list"),h(),v(6," Filters "),h(),f(7,"button",5)(8,"mat-icon"),v(9,"swap_vert"),h(),v(10," Sort "),h()(),f(11,"div",6)(12,"mat-paginator",7),R("page",function(r){Me(e);let o=S();return Te(o.handlePageEvent(r))}),h()()(),f(13,"div",8),st(14,Qj,1,1,"app-product-item",9,Zj),h()(),f(16,"mat-menu",null,0)(18,"mat-selection-list",10),R("selectionChange",function(r){Me(e);let o=S();return Te(o.onSortChange(r))}),st(19,Xj,2,3,"mat-list-option",11,eo),h()()}if(t&2){let e,i,r=Ve(17),o=S();p(7),D("matMenuTriggerFor",r),p(5),D("length",(e=o.products())==null?null:e.count)("pageSize",o.shopParams.pageSize)("showFirstLastButtons",!0)("pageSizeOptions",o.pageSizeOptions)("pageIndex",o.shopParams.pageNumber-1),p(2),at(((i=o.products())==null?null:i.data)??il(7,Yj)),p(4),D("multiple",!1),p(),at(o.sortOptions)}}var vh=class t{shopService=d(Ji);dialogService=d(CI);products=N(null);sortOptions=[{name:"Alphabetical",value:"name"},{name:"Price: Low-High",value:"priceAsc"},{name:"Price: High-Low",value:"priceDesc"}];shopParams=new ao;pageSizeOptions=[5,10,15,20];ngOnInit(){this.initializeShop()}initializeShop(){this.shopService.getBrands(),this.shopService.getCategory(),this.getProducts()}getProducts(){this.shopService.getProducts(this.shopParams).subscribe({next:n=>this.products.set(n),error:n=>console.error(n),complete:()=>console.log("complete")})}onSearchChange(){this.shopParams.pageNumber=1,this.getProducts()}handlePageEvent(n){this.shopParams.pageNumber=n.pageIndex+1,this.shopParams.pageSize=n.pageSize,this.getProducts()}onSortChange(n){let e=n.options[0];e&&(this.shopParams.sort=e.value,this.shopParams.pageNumber=1,this.getProducts(),console.log(this.shopParams.sort))}constructor(){hi(()=>{this.shopParams.search=this.shopService.searchChanged(),this.shopParams.pageNumber=1,this.getProducts()})}openFiltersDialog(){this.dialogService.open(hh,{minWidth:"500px",data:{selectedBrands:this.shopParams.brands,selectedCategory:this.shopParams.category}}).afterClosed().subscribe({next:e=>{e&&(console.log(e),this.shopParams.brands=e.selectedBrands,this.shopParams.category=e.selectedCategory,this.shopParams.pageNumber=1,this.getProducts())}})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-shopping"]],decls:1,vars:1,consts:[["sortMenu","matMenu"],[1,"flex","flex-col","gap-3"],[1,"flex","justify-between","items-center","w-full"],[1,"flex","gap-3","justify-start"],["mat-stroked-button","",3,"click"],["mat-stroked-button","",3,"matMenuTriggerFor"],[1,"flex","justify-end"],["aria-label","Select page",1,"gap-7",3,"page","length","pageSize","showFirstLastButtons","pageSizeOptions","pageIndex"],[1,"grid","grid-cols-5","gap-4"],[3,"product"],[3,"selectionChange","multiple"],[3,"value","selected"]],template:function(e,i){e&1&&F(0,Kj,21,8),e&2&&L(i.products()?0:-1)},dependencies:[$f,Hf,An,bo,fd,gh,md,xk,Dy,vo],encapsulation:2})};var Jj=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2,changeDetection:0})}return t})(),eV={passive:!0},wk=(()=>{class t{_platform=d(Ce);_ngZone=d(j);_renderer=d(Ot).createRenderer(null,null);_styleLoader=d(qe);_monitoredElements=new Map;constructor(){}monitor(e){if(!this._platform.isBrowser)return Et;this._styleLoader.load(Jj);let i=Cn(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new I,s="cdk-text-field-autofilled",a=l=>{l.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,eV)));return this._monitoredElements.set(i,{subject:o,unlisten:c}),o}stopMonitoring(e){let i=Cn(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Dk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({})}return t})();var Ek=new y("MAT_INPUT_VALUE_ACCESSOR");var tV=["button","checkbox","file","hidden","image","radio","range","reset","submit"],nV=new y("MAT_INPUT_CONFIG"),Co=(()=>{class t{_elementRef=d(P);_platform=d(Ce);ngControl=d(xn,{optional:!0,self:!0});_autofillMonitor=d(wk);_ngZone=d(j);_formField=d(vd,{optional:!0});_renderer=d(we);_uid=d(Ie).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=d(nV,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new I;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=At(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(Si.required)??!1}set required(e){this._required=At(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Pb().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=At(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Pb().has(e));constructor(){let e=d(Ms,{optional:!0}),i=d(Ar,{optional:!0}),r=d(yo),o=d(Ek,{optional:!0,self:!0}),s=this._elementRef.nativeElement,a=s.nodeName.toLowerCase();o?Yi(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Ja(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=a==="select",this._isTextarea=a==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&hi(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){tV.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&R("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(ft("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),Y("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),z("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",V]},exportAs:["matInput"],features:[Re([{provide:_d,useExisting:t}]),$e]})}return t})(),Sk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[bd,bd,Dk,ye]})}return t})();function rV(t,n){if(t&1){let e=dt();f(0,"section",0)(1,"div",1)(2,"div",2)(3,"div",3),M(4,"img",4),h(),f(5,"div")(6,"h1",5),v(7),h(),f(8,"p"),v(9),h(),f(10,"div",6)(11,"p",7),v(12),Ee(13,"currency"),h()(),f(14,"div",8)(15,"button",9),R("click",function(){Me(e);let r=S();return Te(r.updateCart())}),f(16,"mat-icon"),v(17,"add_shopping_cart"),h(),v(18),h(),f(19,"mat-form-field",10)(20,"mat-label"),v(21,"Quantity"),h(),f(22,"input",11),yr("ngModelChange",function(r){Me(e);let o=S();return to(o.quantity,r)||(o.quantity=r),Te(r)}),h()()(),M(23,"mat-divider"),f(24,"p",12),v(25),h()()()()()}if(t&2){let e,i,r,o,s=S();p(4),D("src",Xt((e=s.product())==null?null:e.imageUrl),Zn),p(3),Z((i=s.product())==null?null:i.name),p(2),De("You have ",s.quantityInCart," of this item in your cart"),p(3),De(" ",Sn(13,9,(r=s.product())==null?null:r.price)," "),p(3),D("disabled",s.quantity===s.quantityInCart),p(3),De(" ",s.getButtonText()," "),p(4),br("ngModel",s.quantity),p(3),De(" ",(o=s.product())==null?null:o.description," ")}}var bh=class t{shopService=d(Ji);activatedRoute=d(yn);cartSevice=d(it);product=N(null);quantityInCart=0;quantity=1;ngOnInit(){this.loadProduct()}loadProduct(){let n=this.activatedRoute.snapshot.paramMap.get("id");n&&this.shopService.getProduct(+n).subscribe({next:e=>{this.product.set(e),this.updateQuantityInCart()},error:e=>console.log(e)})}updateCart(){let n=this.product();if(n)if(this.quantity>this.quantityInCart){let e=this.quantity-this.quantityInCart;this.quantityInCart+=e,this.cartSevice.addItemtoCart(n,e)}else{let e=this.quantityInCart-this.quantity;this.quantityInCart-=e,this.cartSevice.removeItemFromCart(n.id,e)}}updateQuantityInCart(){this.quantityInCart=this.cartSevice.cart()?.items.find(n=>n.productId===this.product()?.id)?.quantityItems||0,this.quantity=this.quantityInCart||1}getButtonText(){return this.quantityInCart>0?"Update Cart":"Add To Cart"}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-product-details"]],decls:1,vars:1,consts:[[1,"py-8"],[1,"max-w-screen-2xl","px-4","mx-auto"],[1,"grid","grid-cols-2","gap-8"],[1,"max-w-xl","mx-auto"],["alt","product image",1,"w-full",3,"src"],[1,"text-2xl","font-semibold","text-gray-900"],[1,"mt-4","items-center","gap-4","flex"],[1,"text-3xl","font-extrabold","text-gray-900"],[1,"flex","gap-4","mt-6"],["mat-flat-button","",1,"min-h-11",3,"click","disabled"],["appearance","outline",1,"flex"],["matInput","","min","0","type","number",3,"ngModelChange","ngModel"],[1,"mb-6","text-gray-500"]],template:function(e,i){e&1&&F(0,rV,26,11,"section",0),e&2&&L(i.product()?0:-1)},dependencies:[An,pt,Rn,Sk,Co,Hn,Ha,vo,ir,dy,ri,fy,Ts,kt],encapsulation:2})};var yh=class t{item=ha.required();cartService=d(it);incrementQuantity(){this.cartService.addItemtoCart(this.item())}decrementQuantity(){this.cartService.removeItemFromCart(this.item().productId)}removeItemFromCart(){this.cartService.removeItemFromCart(this.item().productId,this.item().quantityItems)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-cart-item"]],inputs:{item:[1,"item"]},decls:26,vars:12,consts:[[1,"rounded-lg","border","border-gray-200","bg-white","p-4","shadow-sm","mb-4"],[1,"flex","item-center","justify-between","gap-6"],[1,"shrink","order-1",3,"routerLink"],["alt","product image",1,"h-20","w-20",3,"src"],[1,"flex","items-center","justify-between","order-3"],[1,"flex","items-center","align-middle","gap-3"],["mat-icon-button","",3,"click"],[1,"text-red-600!"],[1,"font-semibold","text-xl","mb-1"],[1,"text-green-600!"],[1,"text-end","order-4","w-32"],[1,"font-bold","text-xl"],[1,"w-full","flex-1","space-y-3","order-1","max-w-md","gap","mt-2"],[1,"font-medium",3,"routerLink"],[1,"flex","items-center","gap-4","mt-3","translate-y-2.5"],["mat-button","",1,"text-red-700!",3,"click"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"div",1)(2,"a",2),M(3,"img",3),h(),f(4,"div",4)(5,"div",5)(6,"button",6),R("click",function(){return i.decrementQuantity()}),f(7,"mat-icon",7),v(8,"remove"),h()(),f(9,"div",8),v(10),h(),f(11,"button",6),R("click",function(){return i.incrementQuantity()}),f(12,"mat-icon",9),v(13,"add"),h()()(),f(14,"div",10)(15,"p",11),v(16),Ee(17,"currency"),h()()(),f(18,"div",12)(19,"a",13),v(20),h(),f(21,"div",14)(22,"button",15),R("click",function(){return i.removeItemFromCart()}),f(23,"mat-icon"),v(24,"delete"),h(),v(25," Delete "),h()()()()()),e&2&&(p(2),D("routerLink",Xn("/shop/",i.item().productId)),p(),D("src",Xt(i.item().imageUrl),Zn),p(7),Z(i.item().quantityItems),p(6),Z(et(17,9,i.item().price,"GBP")),p(3),D("routerLink",Xn("/shop/",i.item().productId)),p(),De(" ",i.item().productName," "))},dependencies:[nt,pt,An,ws,kt],encapsulation:2})};function oV(t,n){t&1&&(f(0,"div",10)(1,"button",17),v(2,"Checkout"),h(),f(3,"button",18),v(4,"Continue Shopping"),h()())}var ec=class t{cartService=d(it);location=d(Zi);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-order-summary"]],decls:41,vars:17,consts:[[1,"mx-auto","max-w-4xl","flex-1","space-y-6","w-full"],[1,"space-y-4","rounded-lg","border","border-gray-200","p-4","bg-white","shadow-sm"],[1,"text-xl","font-semibold"],[1,"space-y-4"],[1,"space-y-2"],[1,"flex","items-center","justify-between","gap-4"],[1,"font-medium","text-gray-500"],[1,"font-medium","text-gray-900"],[1,"font-medium","text-green-600"],[1,"flex","items-center","justify-between","gap-4","border-t","border-gray-200","pt-2"],[1,"flex","flex-col","gap-2"],[1,"space-y-4","rounded-lg","border","border-gray-200","bg-white","shadow-sm"],[1,"space-y-2","flex","flex-col","p-2"],[1,"mb-2","block","text-sm","font-medium"],["appearance","outline"],["type","text","matInput",""],["mat-flat-button",""],["routerLink","/checkout","mat-flat-button",""],["routerLink","/shop","mat-button",""]],template:function(e,i){if(e&1&&(f(0,"div",0)(1,"div",1)(2,"p",2),v(3,"Order Summary"),h(),f(4,"div",3)(5,"div",4)(6,"dl",5)(7,"dt",6),v(8,"Subtotal"),h(),f(9,"dd",7),v(10),Ee(11,"currency"),h()(),f(12,"dl",5)(13,"dt",6),v(14,"Discount"),h(),f(15,"dd",8),v(16),Ee(17,"currency"),h()(),f(18,"dl",5)(19,"dt",6),v(20,"Delivery Fee"),h(),f(21,"dd",7),v(22),Ee(23,"currency"),h()()(),f(24,"dl",9)(25,"dt",6),v(26,"Total"),h(),f(27,"dd",7),v(28),Ee(29,"currency"),h()()(),F(30,oV,5,0,"div",10),h(),f(31,"div",11)(32,"form",12)(33,"label",13),v(34," Do You Have A Voucher Code? "),h(),f(35,"mat-form-field",14)(36,"mat-label"),v(37,"Voucher Code"),h(),M(38,"input",15),h(),f(39,"button",16),v(40,"Apply Code"),h()()()()),e&2){let r,o,s,a;p(10),Z(et(11,5,(r=i.cartService.totals())==null?null:r.subtotal,"GBP")),p(6),De("-",et(17,8,(o=i.cartService.totals())==null?null:o.discount,"GBP")),p(6),Z(et(23,11,(s=i.cartService.totals())==null?null:s.shipping,"GBP")),p(6),Z(et(29,14,(a=i.cartService.totals())==null?null:a.total,"GBP")),p(2),L(i.location.path()!=="/checkout"?30:-1)}},dependencies:[pt,nt,Rn,Hn,Co,kt],encapsulation:2})};var Ch=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-empty-state"]],decls:8,vars:0,consts:[[1,"max-w-7xl","mx-auto","mt-32","px-10","py-4","bg-white","rounded-lg","shadow-md","w-full"],[1,"flex","flex-col","items-center","justify-center","py-12","w-full"],[1,"icon-display","mb-8"],[1,"text-gray-600","text-lg","font-semibold","mb-4"],["mat-flat-button","","routerLink","/shop"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"div",1)(2,"mat-icon",2),v(3,"shopping_cart"),h(),f(4,"p",3),v(5," Your shopping cart is empty "),h(),f(6,"button",4),v(7,"Go Shopping!"),h()()())},dependencies:[An,pt,nt],styles:[".icon-display[_ngcontent-%COMP%]{transform:scale(3)}"]})};var sV=(t,n)=>n.productId;function aV(t,n){if(t&1&&M(0,"app-cart-item",3),t&2){let e=n.$implicit;D("item",e)}}function cV(t,n){if(t&1&&(f(0,"div",0)(1,"div",1)(2,"div",2),st(3,aV,1,1,"app-cart-item",3,sV),h(),f(5,"div",4),M(6,"app-order-summary"),h()()()),t&2){let e,i=S();p(3),at((e=i.cartService.cart())==null?null:e.items)}}function lV(t,n){t&1&&M(0,"app-empty-state")}var xh=class t{cartService=d(it);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-shoppingcart"]],decls:3,vars:1,consts:[[1,"mx-auto","max-w-7xl"],[1,"flex","w-full","items-start","gap-6","mt-32"],[1,"w-3/4"],[3,"item"],[1,"w-1/4"]],template:function(e,i){if(e&1&&(f(0,"section"),F(1,cV,7,0,"div",0)(2,lV,1,0,"app-empty-state"),h()),e&2){let r;p(),L(((r=i.cartService.cart())==null||r.items==null?null:r.items.length)>0?1:2)}},dependencies:[yh,ec,Ch],encapsulation:2})};var dV=["*"];function uV(t,n){t&1&&se(0)}var Ey=(()=>{class t{_elementRef=d(P);constructor(){}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","cdkStepHeader",""]],hostAttrs:["role","tab"]})}return t})(),Sy=(()=>{class t{template=d(mt);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","cdkStepLabel",""]]})}return t})();var As={NUMBER:"number",EDIT:"edit",DONE:"done",ERROR:"error"},mV=new y("STEPPER_GLOBAL_OPTIONS"),wh=(()=>{class t{_stepperOptions;_stepper=d(tc);_displayDefaultIndicatorType;stepLabel;_childForms;content;stepControl;get interacted(){return this._interacted()}set interacted(e){this._interacted.set(e)}_interacted=N(!1);interactedStream=new H;label;errorMessage;ariaLabel;ariaLabelledby;get state(){return this._state()}set state(e){this._state.set(e)}_state=N(void 0);get editable(){return this._editable()}set editable(e){this._editable.set(e)}_editable=N(!0);optional=!1;get completed(){let e=this._completedOverride(),i=this._interacted();return e??(i&&(!this.stepControl||this.stepControl.valid))}set completed(e){this._completedOverride.set(e)}_completedOverride=N(null);index=N(-1);isSelected=Ct(()=>this._stepper.selectedIndex===this.index());indicatorType=Ct(()=>{let e=this.isSelected(),i=this.completed,r=this._state()??As.NUMBER,o=this._editable();return this._showError()&&this.hasError&&!e?As.ERROR:this._displayDefaultIndicatorType?!i||e?As.NUMBER:o?As.EDIT:As.DONE:i&&!e?As.DONE:i&&e?r:o&&e?As.EDIT:r});isNavigable=Ct(()=>{let e=this.isSelected();return this.completed||e||!this._stepper.linear});get hasError(){let e=this._customError();return e??this._getDefaultError()}set hasError(e){this._customError.set(e)}_customError=N(null);_getDefaultError(){return this.interacted&&!!this.stepControl?.invalid}constructor(){let e=d(mV,{optional:!0});this._stepperOptions=e||{},this._displayDefaultIndicatorType=this._stepperOptions.displayDefaultIndicatorType!==!1}select(){this._stepper.selected=this}reset(){this._interacted.set(!1),this._completedOverride()!=null&&this._completedOverride.set(!1),this._customError()!=null&&this._customError.set(!1),this.stepControl&&(this._childForms?.forEach(e=>e.resetForm?.()),this.stepControl.reset())}ngOnChanges(){this._stepper._stateChanged()}_markAsInteracted(){this._interacted()||(this._interacted.set(!0),this.interactedStream.emit(this))}_showError(){return this._stepperOptions.showError??this._customError()!=null}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["cdk-step"]],contentQueries:function(i,r,o){if(i&1&&yt(o,Sy,5)(o,Ii,5),i&2){let s;U(s=$())&&(r.stepLabel=s.first),U(s=$())&&(r._childForms=s)}},viewQuery:function(i,r){if(i&1&&je(mt,7),i&2){let o;U(o=$())&&(r.content=o.first)}},inputs:{stepControl:"stepControl",label:"label",errorMessage:"errorMessage",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],state:"state",editable:[2,"editable","editable",V],optional:[2,"optional","optional",V],completed:[2,"completed","completed",V],hasError:[2,"hasError","hasError",V]},outputs:{interactedStream:"interacted"},exportAs:["cdkStep"],features:[$e],ngContentSelectors:dV,decls:1,vars:0,template:function(i,r){i&1&&(Se(),da(0,uV,1,0,"ng-template"))},encapsulation:2,changeDetection:0})}return t})(),tc=(()=>{class t{_dir=d(Jt,{optional:!0});_changeDetectorRef=d(ue);_elementRef=d(P);_destroyed=new I;_keyManager;_steps;steps=new Dn;_stepHeader;_sortedHeaders=new Dn;get linear(){return this._linear()}set linear(e){this._linear.set(e)}_linear=N(!1);get selectedIndex(){return this._selectedIndex()}set selectedIndex(e){this._steps?(this._isValidIndex(e),this.selectedIndex!==e&&(this.selected?._markAsInteracted(),!this._anyControlsInvalidOrPending(e)&&(e>=this.selectedIndex||this.steps.toArray()[e].editable)&&this._updateSelectedItemIndex(e))):this._selectedIndex.set(e)}_selectedIndex=N(0);get selected(){return this.steps?this.steps.toArray()[this.selectedIndex]:void 0}set selected(e){this.selectedIndex=e&&this.steps?this.steps.toArray().indexOf(e):-1}selectionChange=new H;selectedIndexChange=new H;_groupId=d(Ie).getId("cdk-stepper-");get orientation(){return this._orientation}set orientation(e){this._orientation=e,this._keyManager&&this._keyManager.withVerticalOrientation(e==="vertical")}_orientation="horizontal";constructor(){}ngAfterContentInit(){this._steps.changes.pipe(Je(this._steps),pe(this._destroyed)).subscribe(e=>{this.steps.reset(e.filter(i=>i._stepper===this)),this.steps.forEach((i,r)=>i.index.set(r)),this.steps.notifyOnChanges()})}ngAfterViewInit(){if(this._stepHeader.changes.pipe(Je(this._stepHeader),pe(this._destroyed)).subscribe(e=>{this._sortedHeaders.reset(e.toArray().sort((i,r)=>i._elementRef.nativeElement.compareDocumentPosition(r._elementRef.nativeElement)&Node.DOCUMENT_POSITION_FOLLOWING?-1:1)),this._sortedHeaders.notifyOnChanges()}),this._keyManager=new Er(this._sortedHeaders).withWrap().withHomeAndEnd().withVerticalOrientation(this._orientation==="vertical"),this._keyManager.updateActiveItem(this.selectedIndex),(this._dir?this._dir.change:Q()).pipe(Je(this._layoutDirection()),pe(this._destroyed)).subscribe(e=>this._keyManager?.withHorizontalOrientation(e)),this._keyManager.updateActiveItem(this.selectedIndex),this.steps.changes.subscribe(()=>{this.selected||this._selectedIndex.set(Math.max(this.selectedIndex-1,0))}),this._isValidIndex(this.selectedIndex)||this._selectedIndex.set(0),this.linear&&this.selectedIndex>0){let e=this.steps.toArray().slice(0,this._selectedIndex());for(let i of e)i._markAsInteracted()}}ngOnDestroy(){this._keyManager?.destroy(),this.steps.destroy(),this._sortedHeaders.destroy(),this._destroyed.next(),this._destroyed.complete()}next(){this.selectedIndex=Math.min(this._selectedIndex()+1,this.steps.length-1)}previous(){this.selectedIndex=Math.max(this._selectedIndex()-1,0)}reset(){this._updateSelectedItemIndex(0),this.steps.forEach(e=>e.reset()),this._stateChanged()}_getStepLabelId(e){return`${this._groupId}-label-${e}`}_getStepContentId(e){return`${this._groupId}-content-${e}`}_stateChanged(){this._changeDetectorRef.markForCheck()}_getAnimationDirection(e){let i=e-this._selectedIndex();return i<0?this._layoutDirection()==="rtl"?"next":"previous":i>0?this._layoutDirection()==="rtl"?"previous":"next":"current"}_getFocusIndex(){return this._keyManager?this._keyManager.activeItemIndex:this._selectedIndex()}_updateSelectedItemIndex(e){let i=this.steps.toArray(),r=this._selectedIndex();this.selectionChange.emit({selectedIndex:e,previouslySelectedIndex:r,selectedStep:i[e],previouslySelectedStep:i[r]}),this._keyManager&&(this._containsFocus()?this._keyManager.setActiveItem(e):this._keyManager.updateActiveItem(e)),this._selectedIndex.set(e),this.selectedIndexChange.emit(e),this._stateChanged()}_onKeydown(e){let i=xt(e),r=e.keyCode,o=this._keyManager;o?.activeItemIndex!=null&&!i&&(r===32||r===13)?(this.selectedIndex=o.activeItemIndex,e.preventDefault()):o?.setFocusOrigin("keyboard").onKeydown(e)}_anyControlsInvalidOrPending(e){return this.linear&&e>=0?this.steps.toArray().slice(0,e).some(i=>{let r=i.stepControl;return(r?r.invalid||r.pending||!i.interacted:!i.completed)&&!i.optional&&!i._completedOverride()}):!1}_layoutDirection(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_containsFocus(){let e=this._elementRef.nativeElement,i=Dr();return e===i||e.contains(i)}_isValidIndex(e){return e>-1&&(!this.steps||e<this.steps.length)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","cdkStepper",""]],contentQueries:function(i,r,o){if(i&1&&yt(o,wh,5)(o,Ey,5),i&2){let s;U(s=$())&&(r._steps=s),U(s=$())&&(r._stepHeader=s)}},inputs:{linear:[2,"linear","linear",V],selectedIndex:[2,"selectedIndex","selectedIndex",ut],selected:"selected",orientation:"orientation"},outputs:{selectionChange:"selectionChange",selectedIndexChange:"selectedIndexChange"},exportAs:["cdkStepper"]})}return t})(),Ik=(()=>{class t{_stepper=d(tc);type="submit";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["button","cdkStepperNext",""]],hostVars:1,hostBindings:function(i,r){i&1&&R("click",function(){return r._stepper.next()}),i&2&&ft("type",r.type)},inputs:{type:"type"}})}return t})(),kk=(()=>{class t{_stepper=d(tc);type="button";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["button","cdkStepperPrevious",""]],hostVars:1,hostBindings:function(i,r){i&1&&R("click",function(){return r._stepper.previous()}),i&2&&ft("type",r.type)},inputs:{type:"type"}})}return t})(),Mk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[ye]})}return t})();var fV=(t,n,e)=>({index:t,active:n,optional:e});function hV(t,n){if(t&1&&cn(0,2),t&2){let e=S();D("ngTemplateOutlet",e.iconOverrides[e.state])("ngTemplateOutletContext",gv(2,fV,e.index,e.active,e.optional))}}function pV(t,n){if(t&1&&(f(0,"span",7),v(1),h()),t&2){let e=S(2);p(),Z(e._getDefaultTextForState(e.state))}}function gV(t,n){if(t&1&&(f(0,"span",8),v(1),h()),t&2){let e=S(3);p(),Z(e._intl.completedLabel)}}function _V(t,n){if(t&1&&(f(0,"span",8),v(1),h()),t&2){let e=S(3);p(),Z(e._intl.editableLabel)}}function vV(t,n){if(t&1&&(F(0,gV,2,1,"span",8)(1,_V,2,1,"span",8),f(2,"mat-icon",7),v(3),h()),t&2){let e=S(2);L(e.state==="done"?0:e.state==="edit"?1:-1),p(3),Z(e._getDefaultTextForState(e.state))}}function bV(t,n){if(t&1&&F(0,pV,2,1,"span",7)(1,vV,4,2),t&2){let e,i=S();L((e=i.state)==="number"?0:1)}}function yV(t,n){t&1&&(f(0,"div",4),cn(1,9),h()),t&2&&(p(),D("ngTemplateOutlet",n.template))}function CV(t,n){if(t&1&&(f(0,"div",4),v(1),h()),t&2){let e=S();p(),Z(e.label)}}function xV(t,n){if(t&1&&(f(0,"div",5),v(1),h()),t&2){let e=S();p(),Z(e._intl.optionalLabel)}}function wV(t,n){if(t&1&&(f(0,"div",6),v(1),h()),t&2){let e=S();p(),Z(e.errorMessage)}}var Tk=["*"];function DV(t,n){}function EV(t,n){if(t&1&&(se(0),Be(1,DV,0,0,"ng-template",0)),t&2){let e=S();p(),D("cdkPortalOutlet",e._portal)}}var SV=["animatedContainer"],Ak=t=>({steps:t}),Rk=t=>({step:t});function IV(t,n){t&1&&se(0)}function kV(t,n){if(t&1&&(f(0,"div",5),cn(1,9)(2,6),h()),t&2){let e=S(2),i=Ve(6);p(),D("ngTemplateOutlet",e.headerPrefix()),p(),D("ngTemplateOutlet",i)("ngTemplateOutletContext",ma(3,Ak,e.steps))}}function MV(t,n){if(t&1&&cn(0,6),t&2){let e=S(2),i=Ve(6);D("ngTemplateOutlet",i)("ngTemplateOutletContext",ma(2,Ak,e.steps))}}function TV(t,n){if(t&1&&(f(0,"div",10,2),cn(2,9),h()),t&2){let e=n.$implicit,i=n.$index,r=S(2);It("mat-horizontal-stepper-content-"+r._getAnimationDirection(i)),D("id",r._getStepContentId(i)),Y("aria-labelledby",r._getStepLabelId(i))("inert",r.selectedIndex===i?null:""),p(2),D("ngTemplateOutlet",e.content)}}function AV(t,n){if(t&1&&(f(0,"div",3),F(1,kV,3,5,"div",5)(2,MV,1,4,"ng-container",6),f(3,"div",7),st(4,TV,3,6,"div",8,cs),h()()),t&2){let e=S();p(),L(e.headerPrefix()?1:2),p(3),at(e.steps)}}function RV(t,n){if(t&1&&cn(0,9),t&2){let e=S(2);D("ngTemplateOutlet",e.headerPrefix())}}function OV(t,n){if(t&1&&(f(0,"div",11),cn(1,6),f(2,"div",12,2)(4,"div",13)(5,"div",14),cn(6,9),h()()()()),t&2){let e=n.$implicit,i=n.$index,r=n.$index,o=n.$count,s=S(2),a=Ve(4);p(),D("ngTemplateOutlet",a)("ngTemplateOutletContext",ma(10,Rk,e)),p(),z("mat-stepper-vertical-line",r!==o-1)("mat-vertical-content-container-active",s.selectedIndex===i),Y("inert",s.selectedIndex===i?null:""),p(2),D("id",s._getStepContentId(i)),Y("aria-labelledby",s._getStepLabelId(i)),p(2),D("ngTemplateOutlet",e.content)}}function NV(t,n){if(t&1&&(f(0,"div",4),F(1,RV,1,1,"ng-container",9),st(2,OV,7,12,"div",11,cs),h()),t&2){let e=S();p(),L(e.headerPrefix()?1:-1),p(),at(e.steps)}}function PV(t,n){if(t&1){let e=dt();f(0,"mat-step-header",15),R("click",function(){let r=Me(e).step;return Te(r.select())})("keydown",function(r){Me(e);let o=S();return Te(o._onKeydown(r))}),h()}if(t&2){let e=n.step,i=S();z("mat-horizontal-stepper-header",i.orientation==="horizontal")("mat-vertical-stepper-header",i.orientation==="vertical"),D("tabIndex",i._getFocusIndex()===e.index()?0:-1)("id",i._getStepLabelId(e.index()))("index",e.index())("state",e.indicatorType())("label",e.stepLabel||e.label)("selected",e.isSelected())("active",e.isNavigable())("optional",e.optional)("errorMessage",e.errorMessage)("iconOverrides",i._iconOverrides)("disableRipple",i.disableRipple||!e.isNavigable())("color",e.color||i.color),Y("role",i.orientation==="horizontal"?"tab":"button")("aria-posinset",i.orientation==="horizontal"?e.index()+1:null)("aria-setsize",i.orientation==="horizontal"?i.steps.length:null)("aria-selected",i.orientation==="horizontal"?e.isSelected():null)("aria-current",i.orientation==="vertical"&&e.isSelected()?"step":null)("aria-disabled",i.orientation==="vertical"&&e.isSelected()?"true":null)("aria-expanded",i.orientation==="vertical"?e.isSelected():null)("aria-controls",i._getStepContentId(e.index()))("aria-label",e.ariaLabel||null)("aria-labelledby",!e.ariaLabel&&e.ariaLabelledby?e.ariaLabelledby:null)("aria-disabled",e.isNavigable()?null:!0)}}function FV(t,n){t&1&&M(0,"div",17)}function LV(t,n){if(t&1&&(cn(0,6),F(1,FV,1,0,"div",17)),t&2){let e=n.$implicit,i=n.$index,r=n.$count;S(2);let o=Ve(4);D("ngTemplateOutlet",o)("ngTemplateOutletContext",ma(3,Rk,e)),p(),L(i!==r-1?1:-1)}}function BV(t,n){if(t&1&&(f(0,"div",16),st(1,LV,2,5,null,null,cs),h()),t&2){let e=n.steps;p(),at(e)}}var Iy=(()=>{class t extends Sy{static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["","matStepLabel",""]],features:[_e]})}return t})(),jV=(()=>{class t{changes=new I;optionalLabel="Optional";completedLabel="Completed";editableLabel="Editable";static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ky=(()=>{class t extends Ey{_intl=d(jV);_focusMonitor=d(Mn);_intlSubscription;state;label;errorMessage;iconOverrides;index;selected=!1;active=!1;optional=!1;disableRipple=!1;color;constructor(){super();let e=d(qe);e.load(Tn),e.load(er);let i=d(ue);this._intlSubscription=this._intl.changes.subscribe(()=>i.markForCheck())}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0)}ngOnDestroy(){this._intlSubscription.unsubscribe(),this._focusMonitor.stopMonitoring(this._elementRef)}focus(e,i){e?this._focusMonitor.focusVia(this._elementRef,e,i):this._elementRef.nativeElement.focus(i)}_stringLabel(){return this.label instanceof Iy?null:this.label}_templateLabel(){return this.label instanceof Iy?this.label:null}_getHostElement(){return this._elementRef.nativeElement}_getDefaultTextForState(e){return e=="number"?`${this.index+1}`:e=="edit"?"create":e=="error"?"warning":e}_hasEmptyLabel(){return!this._stringLabel()&&!this._templateLabel()&&!this._hasOptionalLabel()&&!this._hasErrorLabel()}_hasOptionalLabel(){return this.optional&&this.state!=="error"}_hasErrorLabel(){return this.state==="error"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-step-header"]],hostAttrs:["role","",1,"mat-step-header"],hostVars:4,hostBindings:function(i,r){i&2&&(It("mat-"+(r.color||"primary")),z("mat-step-header-empty-label",r._hasEmptyLabel()))},inputs:{state:"state",label:"label",errorMessage:"errorMessage",iconOverrides:"iconOverrides",index:"index",selected:"selected",active:"active",optional:"optional",disableRipple:"disableRipple",color:"color"},features:[_e],decls:10,vars:17,consts:[["matRipple","",1,"mat-step-header-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"],[1,"mat-step-icon-content"],[3,"ngTemplateOutlet","ngTemplateOutletContext"],[1,"mat-step-label"],[1,"mat-step-text-label"],[1,"mat-step-optional"],[1,"mat-step-sub-label-error"],["aria-hidden","true"],[1,"cdk-visually-hidden"],[3,"ngTemplateOutlet"]],template:function(i,r){if(i&1&&(M(0,"div",0),f(1,"div")(2,"div",1),F(3,hV,1,6,"ng-container",2)(4,bV,2,1),h()(),f(5,"div",3),F(6,yV,2,1,"div",4)(7,CV,2,1,"div",4),F(8,xV,2,1,"div",5),F(9,wV,2,1,"div",6),h()),i&2){let o;D("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disableRipple),p(),It(Xn("mat-step-icon-state-",r.state," mat-step-icon")),z("mat-step-icon-selected",r.selected),p(2),L(r.iconOverrides&&r.iconOverrides[r.state]?3:4),p(2),z("mat-step-label-active",r.active)("mat-step-label-selected",r.selected)("mat-step-label-error",r.state=="error"),p(),L((o=r._templateLabel())?6:r._stringLabel()?7:-1,o),p(2),L(r._hasOptionalLabel()?8:-1),p(),L(r._hasErrorLabel()?9:-1)}},dependencies:[tr,Xi,An],styles:[`.mat-step-header {
  overflow: hidden;
  outline: none;
  cursor: pointer;
  position: relative;
  box-sizing: content-box;
  -webkit-tap-highlight-color: transparent;
}
.mat-step-header:focus-visible .mat-focus-indicator::before {
  content: "";
}
.mat-step-header:hover[aria-disabled=true] {
  cursor: default;
}
.mat-step-header:hover:not([aria-disabled]), .mat-step-header:hover[aria-disabled=false] {
  background-color: var(--mat-stepper-header-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
  border-radius: var(--mat-stepper-header-hover-state-layer-shape, var(--mat-sys-corner-medium));
}
.mat-step-header.cdk-keyboard-focused, .mat-step-header.cdk-program-focused {
  background-color: var(--mat-stepper-header-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  border-radius: var(--mat-stepper-header-focus-state-layer-shape, var(--mat-sys-corner-medium));
}
@media (hover: none) {
  .mat-step-header:hover {
    background: none;
  }
}
@media (forced-colors: active) {
  .mat-step-header {
    outline: solid 1px;
  }
  .mat-step-header[aria-selected=true] .mat-step-label {
    text-decoration: underline;
  }
  .mat-step-header[aria-disabled=true] {
    outline-color: GrayText;
  }
  .mat-step-header[aria-disabled=true] .mat-step-label,
  .mat-step-header[aria-disabled=true] .mat-step-icon,
  .mat-step-header[aria-disabled=true] .mat-step-optional {
    color: GrayText;
  }
}

.mat-step-optional {
  font-size: 12px;
  color: var(--mat-stepper-header-optional-label-text-color, var(--mat-sys-on-surface-variant));
}

.mat-step-sub-label-error {
  font-size: 12px;
  font-weight: normal;
}

.mat-step-icon {
  border-radius: 50%;
  height: 24px;
  width: 24px;
  flex-shrink: 0;
  position: relative;
  color: var(--mat-stepper-header-icon-foreground-color, var(--mat-sys-surface));
  background-color: var(--mat-stepper-header-icon-background-color, var(--mat-sys-on-surface-variant));
}

.mat-step-icon-content {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  display: flex;
}

.mat-step-icon .mat-icon {
  font-size: 16px;
  height: 16px;
  width: 16px;
}

.mat-step-icon-state-error {
  background-color: var(--mat-stepper-header-error-state-icon-background-color, transparent);
  color: var(--mat-stepper-header-error-state-icon-foreground-color, var(--mat-sys-error));
}
.mat-step-icon-state-error .mat-icon {
  font-size: 24px;
  height: 24px;
  width: 24px;
}

.mat-step-label {
  display: inline-block;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  min-width: 50px;
  vertical-align: middle;
  font-family: var(--mat-stepper-header-label-text-font, var(--mat-sys-title-small-font));
  font-size: var(--mat-stepper-header-label-text-size, var(--mat-sys-title-small-size));
  font-weight: var(--mat-stepper-header-label-text-weight, var(--mat-sys-title-small-weight));
  color: var(--mat-stepper-header-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-step-label.mat-step-label-active {
  color: var(--mat-stepper-header-selected-state-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-step-label.mat-step-label-error {
  color: var(--mat-stepper-header-error-state-label-text-color, var(--mat-sys-error));
  font-size: var(--mat-stepper-header-error-state-label-text-size, var(--mat-sys-title-small-size));
}
.mat-step-label.mat-step-label-selected {
  font-size: var(--mat-stepper-header-selected-state-label-text-size, var(--mat-sys-title-small-size));
  font-weight: var(--mat-stepper-header-selected-state-label-text-weight, var(--mat-sys-title-small-weight));
}
.mat-step-header-empty-label .mat-step-label {
  min-width: 0;
}

.mat-step-text-label {
  text-overflow: ellipsis;
  overflow: hidden;
}

.mat-step-header .mat-step-header-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}

.mat-step-icon-selected {
  background-color: var(--mat-stepper-header-selected-state-icon-background-color, var(--mat-sys-primary));
  color: var(--mat-stepper-header-selected-state-icon-foreground-color, var(--mat-sys-on-primary));
}

.mat-step-icon-state-done {
  background-color: var(--mat-stepper-header-done-state-icon-background-color, var(--mat-sys-primary));
  color: var(--mat-stepper-header-done-state-icon-foreground-color, var(--mat-sys-on-primary));
}

.mat-step-icon-state-edit {
  background-color: var(--mat-stepper-header-edit-state-icon-background-color, var(--mat-sys-primary));
  color: var(--mat-stepper-header-edit-state-icon-foreground-color, var(--mat-sys-on-primary));
}
`],encapsulation:2,changeDetection:0})}return t})(),VV=(()=>{class t{templateRef=d(mt);name;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["ng-template","matStepperIcon",""]],inputs:{name:[0,"matStepperIcon","name"]}})}return t})(),HV=(()=>{class t{_template=d(mt);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["ng-template","matStepContent",""]]})}return t})(),My=(()=>{class t extends wh{_errorStateMatcher=d(yo,{skipSelf:!0});_viewContainerRef=d(Nt);_isSelected=ve.EMPTY;stepLabel=void 0;color;_lazyContent;_portal;ngAfterContentInit(){this._isSelected=this._stepper.steps.changes.pipe(_t(()=>this._stepper.selectionChange.pipe(ee(e=>e.selectedStep===this),Je(this._stepper.selected===this)))).subscribe(e=>{e&&this._lazyContent&&!this._portal&&(this._portal=new Vn(this._lazyContent._template,this._viewContainerRef))})}ngOnDestroy(){this._isSelected.unsubscribe()}isErrorState(e,i){let r=this._errorStateMatcher.isErrorState(e,i),o=!!(e&&e.invalid&&this.interacted);return r||o}static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275cmp=E({type:t,selectors:[["mat-step"]],contentQueries:function(i,r,o){if(i&1&&yt(o,Iy,5)(o,HV,5),i&2){let s;U(s=$())&&(r.stepLabel=s.first),U(s=$())&&(r._lazyContent=s.first)}},hostAttrs:["hidden",""],inputs:{color:"color"},exportAs:["matStep"],features:[Re([{provide:yo,useExisting:t},{provide:wh,useExisting:t}]),_e],ngContentSelectors:Tk,decls:1,vars:0,consts:[[3,"cdkPortalOutlet"]],template:function(i,r){i&1&&(Se(),Be(0,EV,2,1,"ng-template"))},dependencies:[nr],encapsulation:2,changeDetection:0})}return t})(),Ty=(()=>{class t extends tc{_ngZone=d(j);_renderer=d(we);_animationsDisabled=ke();_cleanupTransition;_isAnimating=N(!1);_stepHeader=void 0;_animatedContainers;_steps=void 0;steps=new Dn;_icons;animationDone=new H;disableRipple=!1;color;labelPosition="end";headerPosition="top";headerPrefix=ha(null);_iconOverrides={};get animationDuration(){return this._animationDuration}set animationDuration(e){this._animationDuration=/^\d+$/.test(e)?e+"ms":e}_animationDuration="";_isServer=!d(Ce).isBrowser;constructor(){super();let i=d(P).nativeElement.nodeName.toLowerCase();this.orientation=i==="mat-vertical-stepper"?"vertical":"horizontal"}ngAfterContentInit(){super.ngAfterContentInit(),this._icons.forEach(({name:e,templateRef:i})=>this._iconOverrides[e]=i),this.steps.changes.pipe(pe(this._destroyed)).subscribe(()=>this._stateChanged()),this.selectedIndexChange.pipe(pe(this._destroyed)).subscribe(()=>{let e=this._getAnimationDuration();e==="0ms"||e==="0s"?this._onAnimationDone():this._isAnimating.set(!0)}),this._ngZone.runOutsideAngular(()=>{this._animationsDisabled||setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-stepper-animations-enabled"),this._cleanupTransition=this._renderer.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionend)},200)})}ngAfterViewInit(){if(super.ngAfterViewInit(),typeof queueMicrotask=="function"){let e=!1;this._animatedContainers.changes.pipe(Je(null),pe(this._destroyed)).subscribe(()=>queueMicrotask(()=>{e||(e=!0,this.animationDone.emit()),this._stateChanged()}))}}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTransition?.()}_getAnimationDuration(){return this._animationsDisabled?"0ms":this.animationDuration?this.animationDuration:this.orientation==="horizontal"?"500ms":"225ms"}_handleTransitionend=e=>{let i=e.target;if(!i)return;let r=this.orientation==="horizontal"&&e.propertyName==="transform"&&i.classList.contains("mat-horizontal-stepper-content-current"),o=this.orientation==="vertical"&&e.propertyName==="grid-template-rows"&&i.classList.contains("mat-vertical-content-container-active");(r||o)&&this._animatedContainers.find(a=>a.nativeElement===i)&&this._onAnimationDone()};_onAnimationDone(){this._isAnimating.set(!1),this.animationDone.emit()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-stepper"],["mat-vertical-stepper"],["mat-horizontal-stepper"],["","matStepper",""]],contentQueries:function(i,r,o){if(i&1&&yt(o,My,5)(o,VV,5),i&2){let s;U(s=$())&&(r._steps=s),U(s=$())&&(r._icons=s)}},viewQuery:function(i,r){if(i&1&&je(ky,5)(SV,5),i&2){let o;U(o=$())&&(r._stepHeader=o),U(o=$())&&(r._animatedContainers=o)}},hostVars:14,hostBindings:function(i,r){i&2&&(Qn("--mat-stepper-animation-duration",r._getAnimationDuration()),z("mat-stepper-horizontal",r.orientation==="horizontal")("mat-stepper-vertical",r.orientation==="vertical")("mat-stepper-label-position-end",r.orientation==="horizontal"&&r.labelPosition=="end")("mat-stepper-label-position-bottom",r.orientation==="horizontal"&&r.labelPosition=="bottom")("mat-stepper-header-position-bottom",r.headerPosition==="bottom")("mat-stepper-animating",r._isAnimating()))},inputs:{disableRipple:"disableRipple",color:"color",labelPosition:"labelPosition",headerPosition:"headerPosition",headerPrefix:[1,"headerPrefix"],animationDuration:"animationDuration"},outputs:{animationDone:"animationDone"},exportAs:["matStepper","matVerticalStepper","matHorizontalStepper"],features:[Re([{provide:tc,useExisting:t}]),_e],ngContentSelectors:Tk,decls:7,vars:2,consts:[["stepTemplate",""],["horizontalStepsTemplate",""],["animatedContainer",""],[1,"mat-horizontal-stepper-wrapper"],[1,"mat-vertical-stepper-wrapper"],[1,"mat-horizontal-stepper-header-wrapper"],[3,"ngTemplateOutlet","ngTemplateOutletContext"],[1,"mat-horizontal-content-container"],["role","tabpanel",1,"mat-horizontal-stepper-content",3,"id","class"],[3,"ngTemplateOutlet"],["role","tabpanel",1,"mat-horizontal-stepper-content",3,"id"],[1,"mat-step"],[1,"mat-vertical-content-container"],["role","region",1,"mat-vertical-stepper-content",3,"id"],[1,"mat-vertical-content"],[3,"click","keydown","tabIndex","id","index","state","label","selected","active","optional","errorMessage","iconOverrides","disableRipple","color"],["aria-orientation","horizontal","role","tablist",1,"mat-horizontal-stepper-header-container"],[1,"mat-stepper-horizontal-line"]],template:function(i,r){if(i&1&&(Se(),F(0,IV,1,0),F(1,AV,6,1,"div",3)(2,NV,4,1,"div",4),Be(3,PV,1,27,"ng-template",null,0,Kn)(5,BV,3,0,"ng-template",null,1,Kn)),i&2){let o;L(r._isServer?0:-1),p(),L((o=r.orientation)==="horizontal"?1:o==="vertical"?2:-1)}},dependencies:[Xi,ky],styles:[`.mat-stepper-vertical,
.mat-stepper-horizontal {
  display: block;
  font-family: var(--mat-stepper-container-text-font, var(--mat-sys-body-medium-font));
  background: var(--mat-stepper-container-color, var(--mat-sys-surface));
}

.mat-horizontal-stepper-header-wrapper {
  align-items: center;
  display: flex;
}

.mat-horizontal-stepper-header-container {
  white-space: nowrap;
  display: flex;
  align-items: center;
  flex-grow: 1;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header-container {
  align-items: flex-start;
}
.mat-stepper-header-position-bottom .mat-horizontal-stepper-header-container {
  order: 1;
}

.mat-stepper-horizontal-line {
  border-top-width: 1px;
  border-top-style: solid;
  flex: auto;
  height: 0;
  margin: 0 -16px;
  min-width: 32px;
  border-top-color: var(--mat-stepper-line-color, var(--mat-sys-outline));
}
.mat-stepper-label-position-bottom .mat-stepper-horizontal-line {
  margin: 0;
  min-width: 0;
  position: relative;
  top: calc(calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) + 12px);
}

.mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::before, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::before, .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::after, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::after {
  border-top-width: 1px;
  border-top-style: solid;
  content: "";
  display: inline-block;
  height: 0;
  position: absolute;
  width: calc(50% - 20px);
}

.mat-horizontal-stepper-header {
  display: flex;
  overflow: hidden;
  align-items: center;
  padding: 0 24px;
  height: var(--mat-stepper-header-height, 72px);
}
.mat-horizontal-stepper-header .mat-step-icon {
  margin-right: 8px;
  flex: none;
}
[dir=rtl] .mat-horizontal-stepper-header .mat-step-icon {
  margin-right: 0;
  margin-left: 8px;
}
.mat-horizontal-stepper-header.mat-step-header-empty-label .mat-step-icon {
  margin: 0;
}
.mat-horizontal-stepper-header::before, .mat-horizontal-stepper-header::after {
  border-top-color: var(--mat-stepper-line-color, var(--mat-sys-outline));
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header {
  padding: calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) 24px;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header::before, .mat-stepper-label-position-bottom .mat-horizontal-stepper-header::after {
  top: calc(calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) + 12px);
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header {
  box-sizing: border-box;
  flex-direction: column;
  height: auto;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::after, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::after {
  right: 0;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::before, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::before {
  left: 0;
}
[dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:last-child::before, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:first-child::after {
  display: none;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header .mat-step-icon {
  margin-right: 0;
  margin-left: 0;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header .mat-step-label {
  padding: 16px 0 0 0;
  text-align: center;
  width: 100%;
}

.mat-vertical-stepper-header {
  display: flex;
  align-items: center;
  height: 24px;
  padding: calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) 24px;
}
.mat-vertical-stepper-header .mat-step-icon {
  margin-right: 12px;
}
[dir=rtl] .mat-vertical-stepper-header .mat-step-icon {
  margin-right: 0;
  margin-left: 12px;
}

.mat-horizontal-stepper-wrapper {
  display: flex;
  flex-direction: column;
}

.mat-horizontal-stepper-content {
  visibility: hidden;
  overflow: hidden;
  outline: 0;
  height: 0;
}
.mat-stepper-animations-enabled .mat-horizontal-stepper-content {
  transition: transform var(--mat-stepper-animation-duration, 0) cubic-bezier(0.35, 0, 0.25, 1);
}
.mat-horizontal-stepper-content.mat-horizontal-stepper-content-previous {
  transform: translate3d(-100%, 0, 0);
}
.mat-horizontal-stepper-content.mat-horizontal-stepper-content-next {
  transform: translate3d(100%, 0, 0);
}
.mat-horizontal-stepper-content.mat-horizontal-stepper-content-current {
  visibility: visible;
  transform: none;
  height: auto;
}
.mat-stepper-horizontal:not(.mat-stepper-animating) .mat-horizontal-stepper-content.mat-horizontal-stepper-content-current {
  overflow: visible;
}

.mat-horizontal-content-container {
  overflow: hidden;
  padding: 0 24px 24px 24px;
}
@media (forced-colors: active) {
  .mat-horizontal-content-container {
    outline: solid 1px;
  }
}
.mat-stepper-header-position-bottom .mat-horizontal-content-container {
  padding: 24px 24px 0 24px;
}

.mat-vertical-content-container {
  display: grid;
  grid-template-rows: 0fr;
  grid-template-columns: 100%;
  margin-left: 36px;
  border: 0;
  position: relative;
}
.mat-stepper-animations-enabled .mat-vertical-content-container {
  transition: grid-template-rows var(--mat-stepper-animation-duration, 0) cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-vertical-content-container.mat-vertical-content-container-active {
  grid-template-rows: 1fr;
}
.mat-step:last-child .mat-vertical-content-container {
  border: none;
}
@media (forced-colors: active) {
  .mat-vertical-content-container {
    outline: solid 1px;
  }
}
[dir=rtl] .mat-vertical-content-container {
  margin-left: 0;
  margin-right: 36px;
}
@supports not (grid-template-rows: 0fr) {
  .mat-vertical-content-container {
    height: 0;
  }
  .mat-vertical-content-container.mat-vertical-content-container-active {
    height: auto;
  }
}

.mat-stepper-vertical-line::before {
  content: "";
  position: absolute;
  left: 0;
  border-left-width: 1px;
  border-left-style: solid;
  border-left-color: var(--mat-stepper-line-color, var(--mat-sys-outline));
  top: calc(8px - calc((var(--mat-stepper-header-height, 72px) - 24px) / 2));
  bottom: calc(8px - calc((var(--mat-stepper-header-height, 72px) - 24px) / 2));
}
[dir=rtl] .mat-stepper-vertical-line::before {
  left: auto;
  right: 0;
}

.mat-vertical-stepper-content {
  overflow: hidden;
  outline: 0;
  visibility: hidden;
}
.mat-stepper-animations-enabled .mat-vertical-stepper-content {
  transition: visibility var(--mat-stepper-animation-duration, 0) linear;
}
.mat-vertical-content-container-active > .mat-vertical-stepper-content {
  visibility: visible;
}

.mat-vertical-content {
  padding: 0 24px 24px 24px;
}
`],encapsulation:2,changeDetection:0})}return t})(),Ok=(()=>{class t extends Ik{static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["button","matStepperNext",""]],hostAttrs:[1,"mat-stepper-next"],hostVars:1,hostBindings:function(i,r){i&2&&ft("type",r.type)},features:[_e]})}return t})(),Nk=(()=>{class t extends kk{static \u0275fac=(()=>{let e;return function(r){return(e||(e=lt(t)))(r||t)}})();static \u0275dir=T({type:t,selectors:[["button","matStepperPrevious",""]],hostAttrs:[1,"mat-stepper-previous"],hostVars:1,hostBindings:function(i,r){i&2&&ft("type",r.type)},features:[_e]})}return t})(),Pk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({providers:[yo],imports:[Ss,Mk,Uf,uo,Ty,ky,ye]})}return t})();function Sh(t){"@babel/helpers - typeof";return typeof Symbol=="function"&&typeof Symbol.iterator=="symbol"?Sh=function(n){return typeof n}:Sh=function(n){return n&&typeof Symbol=="function"&&n.constructor===Symbol&&n!==Symbol.prototype?"symbol":typeof n},Sh(t)}var Bk="dahlia",UV=function(n){return n===3?"v3":n},jk="https://js.stripe.com",$V="".concat(jk,"/").concat(Bk,"/stripe.js"),GV=/^https:\/\/js\.stripe\.com\/v3\/?(\?.*)?$/,WV=/^https:\/\/js\.stripe\.com\/(v3|[a-z]+)\/stripe\.js(\?.*)?$/,Fk="loadStripe.setLoadParameters was called but an existing Stripe.js script already exists in the document; existing script parameters will be used",qV=function(n){return GV.test(n)||WV.test(n)},YV=function(){for(var n=document.querySelectorAll('script[src^="'.concat(jk,'"]')),e=0;e<n.length;e++){var i=n[e];if(qV(i.src))return i}return null},Lk=function(n){var e=n&&!n.advancedFraudSignals?"?advancedFraudSignals=false":"",i=document.createElement("script");i.src="".concat($V).concat(e);var r=document.head||document.body;if(!r)throw new Error("Expected document.body not to be null. Stripe.js requires a <body> element.");return r.appendChild(i),i},ZV=function(n,e){!n||!n._registerWrapper||n._registerWrapper({name:"stripe-js",version:"9.4.0",startTime:e})},yd=null,Dh=null,Eh=null,QV=function(n){return function(e){n(new Error("Failed to load Stripe.js",{cause:e}))}},XV=function(n,e){return function(){window.Stripe?n(window.Stripe):e(new Error("Stripe.js not available"))}},KV=function(n){return yd!==null?yd:(yd=new Promise(function(e,i){if(typeof window>"u"||typeof document>"u"){e(null);return}if(window.Stripe&&n&&console.warn(Fk),window.Stripe){e(window.Stripe);return}try{var r=YV();if(r&&n)console.warn(Fk);else if(!r)r=Lk(n);else if(r&&Eh!==null&&Dh!==null){var o;r.removeEventListener("load",Eh),r.removeEventListener("error",Dh),(o=r.parentNode)===null||o===void 0||o.removeChild(r),r=Lk(n)}Eh=XV(e,i),Dh=QV(i),r.addEventListener("load",Eh),r.addEventListener("error",Dh)}catch(s){i(s);return}}),yd.catch(function(e){return yd=null,Promise.reject(e)}))},JV=function(n,e,i){if(n===null)return null;var r=e[0];if(typeof r!="string")throw new Error("Expected publishable key to be of type string, got type ".concat(Sh(r)," instead."));var o=r.match(/^pk_test/),s=UV(n.version),a=Bk;o&&s!==a&&console.warn("Stripe.js@".concat(s," was loaded on the page, but @stripe/stripe-js@").concat("9.4.0"," expected Stripe.js@").concat(a,". This may result in unexpected behavior. For more information, see https://docs.stripe.com/sdks/stripejs-versioning"));var c=n.apply(void 0,e);return ZV(c,i),c},Cd,Vk=!1,Hk=function(){return Cd||(Cd=KV(null).catch(function(n){return Cd=null,Promise.reject(n)}),Cd)};Promise.resolve().then(function(){return Hk()}).catch(function(t){Vk||console.warn(t)});var zk=function(){for(var n=arguments.length,e=new Array(n),i=0;i<n;i++)e[i]=arguments[i];Vk=!0;var r=Date.now();return Hk().then(function(o){return JV(o,e,r)})};var zn=class extends Error{constructor(n,e){let i=new.target.prototype;super(`${n}: Status code '${e}'`),this.statusCode=e,this.__proto__=i}},xo=class extends Error{constructor(n="A timeout occurred."){let e=new.target.prototype;super(n),this.__proto__=e}},Yt=class extends Error{constructor(n="An abort occurred."){let e=new.target.prototype;super(n),this.__proto__=e}},Ih=class extends Error{constructor(n,e){let i=new.target.prototype;super(n),this.transport=e,this.errorType="UnsupportedTransportError",this.__proto__=i}},kh=class extends Error{constructor(n,e){let i=new.target.prototype;super(n),this.transport=e,this.errorType="DisabledTransportError",this.__proto__=i}},Mh=class extends Error{constructor(n,e){let i=new.target.prototype;super(n),this.transport=e,this.errorType="FailedToStartTransportError",this.__proto__=i}},xd=class extends Error{constructor(n){let e=new.target.prototype;super(n),this.errorType="FailedToNegotiateWithServerError",this.__proto__=e}},Th=class extends Error{constructor(n,e){let i=new.target.prototype;super(n),this.innerErrors=e,this.__proto__=i}};var nc=class{constructor(n,e,i){this.statusCode=n,this.statusText=e,this.content=i}},rr=class{get(n,e){return this.send(J(b({},e),{method:"GET",url:n}))}post(n,e){return this.send(J(b({},e),{method:"POST",url:n}))}delete(n,e){return this.send(J(b({},e),{method:"DELETE",url:n}))}getCookieString(n){return""}};var w=(function(t){return t[t.Trace=0]="Trace",t[t.Debug=1]="Debug",t[t.Information=2]="Information",t[t.Warning=3]="Warning",t[t.Error=4]="Error",t[t.Critical=5]="Critical",t[t.None=6]="None",t})(w||{});var or=class{constructor(){}log(n,e){}};or.instance=new or;var Uk="10.0.0";var Ye=class{static isRequired(n,e){if(n==null)throw new Error(`The '${e}' argument is required.`)}static isNotEmpty(n,e){if(!n||n.match(/^\s*$/))throw new Error(`The '${e}' argument should not be empty.`)}static isIn(n,e,i){if(!(n in e))throw new Error(`Unknown ${i} value: ${n}.`)}},Qe=class t{static get isBrowser(){return!t.isNode&&typeof window=="object"&&typeof window.document=="object"}static get isWebWorker(){return!t.isNode&&typeof self=="object"&&"importScripts"in self}static get isReactNative(){return!t.isNode&&typeof window=="object"&&typeof window.document>"u"}static get isNode(){return typeof process<"u"&&process.release&&process.release.name==="node"}};function wo(t,n){let e="";return Mi(t)?(e=`Binary data of length ${t.byteLength}`,n&&(e+=`. Content: '${eH(t)}'`)):typeof t=="string"&&(e=`String data of length ${t.length}`,n&&(e+=`. Content: '${t}'`)),e}function eH(t){let n=new Uint8Array(t),e="";return n.forEach(i=>{let r=i<16?"0":"";e+=`0x${r}${i.toString(16)} `}),e.substring(0,e.length-1)}function Mi(t){return t&&typeof ArrayBuffer<"u"&&(t instanceof ArrayBuffer||t.constructor&&t.constructor.name==="ArrayBuffer")}async function Rh(t,n,e,i,r,o){let s={},[a,c]=sr();s[a]=c,t.log(w.Trace,`(${n} transport) sending data. ${wo(r,o.logMessageContent)}.`);let l=Mi(r)?"arraybuffer":"text",u=await e.post(i,{content:r,headers:b(b({},s),o.headers),responseType:l,timeout:o.timeout,withCredentials:o.withCredentials});t.log(w.Trace,`(${n} transport) request complete. Response status: ${u.statusCode}.`)}function $k(t){return t===void 0?new Rs(w.Information):t===null?or.instance:t.log!==void 0?t:new Rs(t)}var Ah=class{constructor(n,e){this._subject=n,this._observer=e}dispose(){let n=this._subject.observers.indexOf(this._observer);n>-1&&this._subject.observers.splice(n,1),this._subject.observers.length===0&&this._subject.cancelCallback&&this._subject.cancelCallback().catch(e=>{})}},Rs=class{constructor(n){this._minLevel=n,this.out=console}log(n,e){if(n>=this._minLevel){let i=`[${new Date().toISOString()}] ${w[n]}: ${e}`;switch(n){case w.Critical:case w.Error:this.out.error(i);break;case w.Warning:this.out.warn(i);break;case w.Information:this.out.info(i);break;default:this.out.log(i);break}}}};function sr(){let t="X-SignalR-User-Agent";return Qe.isNode&&(t="User-Agent"),[t,tH(Uk,nH(),rH(),iH())]}function tH(t,n,e,i){let r="Microsoft SignalR/",o=t.split(".");return r+=`${o[0]}.${o[1]}`,r+=` (${t}; `,n&&n!==""?r+=`${n}; `:r+="Unknown OS; ",r+=`${e}`,i?r+=`; ${i}`:r+="; Unknown Runtime Version",r+=")",r}function nH(){if(Qe.isNode)switch(process.platform){case"win32":return"Windows NT";case"darwin":return"macOS";case"linux":return"Linux";default:return process.platform}else return""}function iH(){if(Qe.isNode)return process.versions.node}function rH(){return Qe.isNode?"NodeJS":"Browser"}function Oh(t){return t.stack?t.stack:t.message?t.message:`${t}`}function Gk(){if(typeof globalThis<"u")return globalThis;if(typeof self<"u")return self;if(typeof window<"u")return window;if(typeof global<"u")return global;throw new Error("could not find global")}var Nh=class extends rr{constructor(n){if(super(),this._logger=n,typeof fetch>"u"||Qe.isNode){let e=typeof __webpack_require__=="function"?__non_webpack_require__:Ns;this._jar=new(e("tough-cookie")).CookieJar,typeof fetch>"u"?this._fetchType=e("node-fetch"):this._fetchType=fetch,this._fetchType=e("fetch-cookie")(this._fetchType,this._jar)}else this._fetchType=fetch.bind(Gk());if(typeof AbortController>"u"){let e=typeof __webpack_require__=="function"?__non_webpack_require__:Ns;this._abortControllerType=e("abort-controller")}else this._abortControllerType=AbortController}async send(n){if(n.abortSignal&&n.abortSignal.aborted)throw new Yt;if(!n.method)throw new Error("No method defined.");if(!n.url)throw new Error("No url defined.");let e=new this._abortControllerType,i;n.abortSignal&&(n.abortSignal.onabort=()=>{e.abort(),i=new Yt});let r=null;if(n.timeout){let c=n.timeout;r=setTimeout(()=>{e.abort(),this._logger.log(w.Warning,"Timeout from HTTP request."),i=new xo},c)}n.content===""&&(n.content=void 0),n.content&&(n.headers=n.headers||{},Mi(n.content)?n.headers["Content-Type"]="application/octet-stream":n.headers["Content-Type"]="text/plain;charset=UTF-8");let o;try{o=await this._fetchType(n.url,{body:n.content,cache:"no-cache",credentials:n.withCredentials===!0?"include":"same-origin",headers:b({"X-Requested-With":"XMLHttpRequest"},n.headers),method:n.method,mode:"cors",redirect:"follow",signal:e.signal})}catch(c){throw i||(this._logger.log(w.Warning,`Error from HTTP request. ${c}.`),c)}finally{r&&clearTimeout(r),n.abortSignal&&(n.abortSignal.onabort=null)}if(!o.ok){let c=await Wk(o,"text");throw new zn(c||o.statusText,o.status)}let a=await Wk(o,n.responseType);return new nc(o.status,o.statusText,a)}getCookieString(n){let e="";return Qe.isNode&&this._jar&&this._jar.getCookies(n,(i,r)=>e=r.join("; ")),e}};function Wk(t,n){let e;switch(n){case"arraybuffer":e=t.arrayBuffer();break;case"text":e=t.text();break;case"blob":case"document":case"json":throw new Error(`${n} is not supported.`);default:e=t.text();break}return e}var Ph=class extends rr{constructor(n){super(),this._logger=n}send(n){return n.abortSignal&&n.abortSignal.aborted?Promise.reject(new Yt):n.method?n.url?new Promise((e,i)=>{let r=new XMLHttpRequest;r.open(n.method,n.url,!0),r.withCredentials=n.withCredentials===void 0?!0:n.withCredentials,r.setRequestHeader("X-Requested-With","XMLHttpRequest"),n.content===""&&(n.content=void 0),n.content&&(Mi(n.content)?r.setRequestHeader("Content-Type","application/octet-stream"):r.setRequestHeader("Content-Type","text/plain;charset=UTF-8"));let o=n.headers;o&&Object.keys(o).forEach(s=>{r.setRequestHeader(s,o[s])}),n.responseType&&(r.responseType=n.responseType),n.abortSignal&&(n.abortSignal.onabort=()=>{r.abort(),i(new Yt)}),n.timeout&&(r.timeout=n.timeout),r.onload=()=>{n.abortSignal&&(n.abortSignal.onabort=null),r.status>=200&&r.status<300?e(new nc(r.status,r.statusText,r.response||r.responseText)):i(new zn(r.response||r.responseText||r.statusText,r.status))},r.onerror=()=>{this._logger.log(w.Warning,`Error from HTTP request. ${r.status}: ${r.statusText}.`),i(new zn(r.statusText,r.status))},r.ontimeout=()=>{this._logger.log(w.Warning,"Timeout from HTTP request."),i(new xo)},r.send(n.content)}):Promise.reject(new Error("No url defined.")):Promise.reject(new Error("No method defined."))}};var Fh=class extends rr{constructor(n){if(super(),typeof fetch<"u"||Qe.isNode)this._httpClient=new Nh(n);else if(typeof XMLHttpRequest<"u")this._httpClient=new Ph(n);else throw new Error("No usable HttpClient found.")}send(n){return n.abortSignal&&n.abortSignal.aborted?Promise.reject(new Yt):n.method?n.url?this._httpClient.send(n):Promise.reject(new Error("No url defined.")):Promise.reject(new Error("No method defined."))}getCookieString(n){return this._httpClient.getCookieString(n)}};var Un=class t{static write(n){return`${n}${t.RecordSeparator}`}static parse(n){if(n[n.length-1]!==t.RecordSeparator)throw new Error("Message is incomplete.");let e=n.split(t.RecordSeparator);return e.pop(),e}};Un.RecordSeparatorCode=30;Un.RecordSeparator=String.fromCharCode(Un.RecordSeparatorCode);var Lh=class{writeHandshakeRequest(n){return Un.write(JSON.stringify(n))}parseHandshakeResponse(n){let e,i;if(Mi(n)){let a=new Uint8Array(n),c=a.indexOf(Un.RecordSeparatorCode);if(c===-1)throw new Error("Message is incomplete.");let l=c+1;e=String.fromCharCode.apply(null,Array.prototype.slice.call(a.slice(0,l))),i=a.byteLength>l?a.slice(l).buffer:null}else{let a=n,c=a.indexOf(Un.RecordSeparator);if(c===-1)throw new Error("Message is incomplete.");let l=c+1;e=a.substring(0,l),i=a.length>l?a.substring(l):null}let r=Un.parse(e),o=JSON.parse(r[0]);if(o.type)throw new Error("Expected a handshake response from the server.");return[i,o]}};var le=(function(t){return t[t.Invocation=1]="Invocation",t[t.StreamItem=2]="StreamItem",t[t.Completion=3]="Completion",t[t.StreamInvocation=4]="StreamInvocation",t[t.CancelInvocation=5]="CancelInvocation",t[t.Ping=6]="Ping",t[t.Close=7]="Close",t[t.Ack=8]="Ack",t[t.Sequence=9]="Sequence",t})(le||{});var Bh=class{constructor(){this.observers=[]}next(n){for(let e of this.observers)e.next(n)}error(n){for(let e of this.observers)e.error&&e.error(n)}complete(){for(let n of this.observers)n.complete&&n.complete()}subscribe(n){return this.observers.push(n),new Ah(this,n)}};var jh=class{constructor(n,e,i){this._bufferSize=1e5,this._messages=[],this._totalMessageCount=0,this._waitForSequenceMessage=!1,this._nextReceivingSequenceId=1,this._latestReceivedSequenceId=0,this._bufferedByteCount=0,this._reconnectInProgress=!1,this._protocol=n,this._connection=e,this._bufferSize=i}async _send(n){let e=this._protocol.writeMessage(n),i=Promise.resolve();if(this._isInvocationMessage(n)){this._totalMessageCount++;let r=()=>{},o=()=>{};Mi(e)?this._bufferedByteCount+=e.byteLength:this._bufferedByteCount+=e.length,this._bufferedByteCount>=this._bufferSize&&(i=new Promise((s,a)=>{r=s,o=a})),this._messages.push(new Ay(e,this._totalMessageCount,r,o))}try{this._reconnectInProgress||await this._connection.send(e)}catch{this._disconnected()}await i}_ack(n){let e=-1;for(let i=0;i<this._messages.length;i++){let r=this._messages[i];if(r._id<=n.sequenceId)e=i,Mi(r._message)?this._bufferedByteCount-=r._message.byteLength:this._bufferedByteCount-=r._message.length,r._resolver();else if(this._bufferedByteCount<this._bufferSize)r._resolver();else break}e!==-1&&(this._messages=this._messages.slice(e+1))}_shouldProcessMessage(n){if(this._waitForSequenceMessage)return n.type!==le.Sequence?!1:(this._waitForSequenceMessage=!1,!0);if(!this._isInvocationMessage(n))return!0;let e=this._nextReceivingSequenceId;return this._nextReceivingSequenceId++,e<=this._latestReceivedSequenceId?(e===this._latestReceivedSequenceId&&this._ackTimer(),!1):(this._latestReceivedSequenceId=e,this._ackTimer(),!0)}_resetSequence(n){if(n.sequenceId>this._nextReceivingSequenceId){this._connection.stop(new Error("Sequence ID greater than amount of messages we've received."));return}this._nextReceivingSequenceId=n.sequenceId}_disconnected(){this._reconnectInProgress=!0,this._waitForSequenceMessage=!0}async _resend(){let n=this._messages.length!==0?this._messages[0]._id:this._totalMessageCount+1;await this._connection.send(this._protocol.writeMessage({type:le.Sequence,sequenceId:n}));let e=this._messages;for(let i of e)await this._connection.send(i._message);this._reconnectInProgress=!1}_dispose(n){n??(n=new Error("Unable to reconnect to server."));for(let e of this._messages)e._rejector(n)}_isInvocationMessage(n){switch(n.type){case le.Invocation:case le.StreamItem:case le.Completion:case le.StreamInvocation:case le.CancelInvocation:return!0;case le.Close:case le.Sequence:case le.Ping:case le.Ack:return!1}}_ackTimer(){this._ackTimerHandle===void 0&&(this._ackTimerHandle=setTimeout(async()=>{try{this._reconnectInProgress||await this._connection.send(this._protocol.writeMessage({type:le.Ack,sequenceId:this._latestReceivedSequenceId}))}catch{}clearTimeout(this._ackTimerHandle),this._ackTimerHandle=void 0},1e3))}},Ay=class{constructor(n,e,i,r){this._message=n,this._id=e,this._resolver=i,this._rejector=r}};var oH=30*1e3,sH=15*1e3,aH=1e5,Xe=(function(t){return t.Disconnected="Disconnected",t.Connecting="Connecting",t.Connected="Connected",t.Disconnecting="Disconnecting",t.Reconnecting="Reconnecting",t})(Xe||{}),wd=class t{static create(n,e,i,r,o,s,a){return new t(n,e,i,r,o,s,a)}constructor(n,e,i,r,o,s,a){this._nextKeepAlive=0,this._freezeEventListener=()=>{this._logger.log(w.Warning,"The page is being frozen, this will likely lead to the connection being closed and messages being lost. For more information see the docs at https://learn.microsoft.com/aspnet/core/signalr/javascript-client#bsleep")},Ye.isRequired(n,"connection"),Ye.isRequired(e,"logger"),Ye.isRequired(i,"protocol"),this.serverTimeoutInMilliseconds=o??oH,this.keepAliveIntervalInMilliseconds=s??sH,this._statefulReconnectBufferSize=a??aH,this._logger=e,this._protocol=i,this.connection=n,this._reconnectPolicy=r,this._handshakeProtocol=new Lh,this.connection.onreceive=c=>this._processIncomingData(c),this.connection.onclose=c=>this._connectionClosed(c),this._callbacks={},this._methods={},this._closedCallbacks=[],this._reconnectingCallbacks=[],this._reconnectedCallbacks=[],this._invocationId=0,this._receivedHandshakeResponse=!1,this._connectionState=Xe.Disconnected,this._connectionStarted=!1,this._cachedPingMessage=this._protocol.writeMessage({type:le.Ping})}get state(){return this._connectionState}get connectionId(){return this.connection&&this.connection.connectionId||null}get baseUrl(){return this.connection.baseUrl||""}set baseUrl(n){if(this._connectionState!==Xe.Disconnected&&this._connectionState!==Xe.Reconnecting)throw new Error("The HubConnection must be in the Disconnected or Reconnecting state to change the url.");if(!n)throw new Error("The HubConnection url must be a valid url.");this.connection.baseUrl=n}start(){return this._startPromise=this._startWithStateTransitions(),this._startPromise}async _startWithStateTransitions(){if(this._connectionState!==Xe.Disconnected)return Promise.reject(new Error("Cannot start a HubConnection that is not in the 'Disconnected' state."));this._connectionState=Xe.Connecting,this._logger.log(w.Debug,"Starting HubConnection.");try{await this._startInternal(),Qe.isBrowser&&window.document.addEventListener("freeze",this._freezeEventListener),this._connectionState=Xe.Connected,this._connectionStarted=!0,this._logger.log(w.Debug,"HubConnection connected successfully.")}catch(n){return this._connectionState=Xe.Disconnected,this._logger.log(w.Debug,`HubConnection failed to start successfully because of error '${n}'.`),Promise.reject(n)}}async _startInternal(){this._stopDuringStartError=void 0,this._receivedHandshakeResponse=!1;let n=new Promise((e,i)=>{this._handshakeResolver=e,this._handshakeRejecter=i});await this.connection.start(this._protocol.transferFormat);try{let e=this._protocol.version;this.connection.features.reconnect||(e=1);let i={protocol:this._protocol.name,version:e};if(this._logger.log(w.Debug,"Sending handshake request."),await this._sendMessage(this._handshakeProtocol.writeHandshakeRequest(i)),this._logger.log(w.Information,`Using HubProtocol '${this._protocol.name}'.`),this._cleanupTimeout(),this._resetTimeoutPeriod(),this._resetKeepAliveInterval(),await n,this._stopDuringStartError)throw this._stopDuringStartError;this.connection.features.reconnect&&(this._messageBuffer=new jh(this._protocol,this.connection,this._statefulReconnectBufferSize),this.connection.features.disconnected=this._messageBuffer._disconnected.bind(this._messageBuffer),this.connection.features.resend=()=>{if(this._messageBuffer)return this._messageBuffer._resend()}),this.connection.features.inherentKeepAlive||await this._sendMessage(this._cachedPingMessage)}catch(e){throw this._logger.log(w.Debug,`Hub handshake failed with error '${e}' during start(). Stopping HubConnection.`),this._cleanupTimeout(),this._cleanupPingTimer(),await this.connection.stop(e),e}}async stop(){let n=this._startPromise;this.connection.features.reconnect=!1,this._stopPromise=this._stopInternal(),await this._stopPromise;try{await n}catch{}}_stopInternal(n){if(this._connectionState===Xe.Disconnected)return this._logger.log(w.Debug,`Call to HubConnection.stop(${n}) ignored because it is already in the disconnected state.`),Promise.resolve();if(this._connectionState===Xe.Disconnecting)return this._logger.log(w.Debug,`Call to HttpConnection.stop(${n}) ignored because the connection is already in the disconnecting state.`),this._stopPromise;let e=this._connectionState;return this._connectionState=Xe.Disconnecting,this._logger.log(w.Debug,"Stopping HubConnection."),this._reconnectDelayHandle?(this._logger.log(w.Debug,"Connection stopped during reconnect delay. Done reconnecting."),clearTimeout(this._reconnectDelayHandle),this._reconnectDelayHandle=void 0,this._completeClose(),Promise.resolve()):(e===Xe.Connected&&this._sendCloseMessage(),this._cleanupTimeout(),this._cleanupPingTimer(),this._stopDuringStartError=n||new Yt("The connection was stopped before the hub handshake could complete."),this.connection.stop(n))}async _sendCloseMessage(){try{await this._sendWithProtocol(this._createCloseMessage())}catch{}}stream(n,...e){let[i,r]=this._replaceStreamingParams(e),o=this._createStreamInvocation(n,e,r),s,a=new Bh;return a.cancelCallback=()=>{let c=this._createCancelInvocation(o.invocationId);return delete this._callbacks[o.invocationId],s.then(()=>this._sendWithProtocol(c))},this._callbacks[o.invocationId]=(c,l)=>{if(l){a.error(l);return}else c&&(c.type===le.Completion?c.error?a.error(new Error(c.error)):a.complete():a.next(c.item))},s=this._sendWithProtocol(o).catch(c=>{a.error(c),delete this._callbacks[o.invocationId]}),this._launchStreams(i,s),a}_sendMessage(n){return this._resetKeepAliveInterval(),this.connection.send(n)}_sendWithProtocol(n){return this._messageBuffer?this._messageBuffer._send(n):this._sendMessage(this._protocol.writeMessage(n))}send(n,...e){let[i,r]=this._replaceStreamingParams(e),o=this._sendWithProtocol(this._createInvocation(n,e,!0,r));return this._launchStreams(i,o),o}invoke(n,...e){let[i,r]=this._replaceStreamingParams(e),o=this._createInvocation(n,e,!1,r);return new Promise((a,c)=>{this._callbacks[o.invocationId]=(u,m)=>{if(m){c(m);return}else u&&(u.type===le.Completion?u.error?c(new Error(u.error)):a(u.result):c(new Error(`Unexpected message type: ${u.type}`)))};let l=this._sendWithProtocol(o).catch(u=>{c(u),delete this._callbacks[o.invocationId]});this._launchStreams(i,l)})}on(n,e){!n||!e||(n=n.toLowerCase(),this._methods[n]||(this._methods[n]=[]),this._methods[n].indexOf(e)===-1&&this._methods[n].push(e))}off(n,e){if(!n)return;n=n.toLowerCase();let i=this._methods[n];if(i)if(e){let r=i.indexOf(e);r!==-1&&(i.splice(r,1),i.length===0&&delete this._methods[n])}else delete this._methods[n]}onclose(n){n&&this._closedCallbacks.push(n)}onreconnecting(n){n&&this._reconnectingCallbacks.push(n)}onreconnected(n){n&&this._reconnectedCallbacks.push(n)}_processIncomingData(n){if(this._cleanupTimeout(),this._receivedHandshakeResponse||(n=this._processHandshakeResponse(n),this._receivedHandshakeResponse=!0),n){let e=this._protocol.parseMessages(n,this._logger);for(let i of e)if(!(this._messageBuffer&&!this._messageBuffer._shouldProcessMessage(i)))switch(i.type){case le.Invocation:this._invokeClientMethod(i).catch(r=>{this._logger.log(w.Error,`Invoke client method threw error: ${Oh(r)}`)});break;case le.StreamItem:case le.Completion:{let r=this._callbacks[i.invocationId];if(r){i.type===le.Completion&&delete this._callbacks[i.invocationId];try{r(i)}catch(o){this._logger.log(w.Error,`Stream callback threw error: ${Oh(o)}`)}}break}case le.Ping:break;case le.Close:{this._logger.log(w.Information,"Close message received from server.");let r=i.error?new Error("Server returned an error on close: "+i.error):void 0;i.allowReconnect===!0?this.connection.stop(r):this._stopPromise=this._stopInternal(r);break}case le.Ack:this._messageBuffer&&this._messageBuffer._ack(i);break;case le.Sequence:this._messageBuffer&&this._messageBuffer._resetSequence(i);break;default:this._logger.log(w.Warning,`Invalid message type: ${i.type}.`);break}}this._resetTimeoutPeriod()}_processHandshakeResponse(n){let e,i;try{[i,e]=this._handshakeProtocol.parseHandshakeResponse(n)}catch(r){let o="Error parsing handshake response: "+r;this._logger.log(w.Error,o);let s=new Error(o);throw this._handshakeRejecter(s),s}if(e.error){let r="Server returned handshake error: "+e.error;this._logger.log(w.Error,r);let o=new Error(r);throw this._handshakeRejecter(o),o}else this._logger.log(w.Debug,"Server handshake complete.");return this._handshakeResolver(),i}_resetKeepAliveInterval(){this.connection.features.inherentKeepAlive||(this._nextKeepAlive=new Date().getTime()+this.keepAliveIntervalInMilliseconds,this._cleanupPingTimer())}_resetTimeoutPeriod(){if(!this.connection.features||!this.connection.features.inherentKeepAlive){this._timeoutHandle=setTimeout(()=>this.serverTimeout(),this.serverTimeoutInMilliseconds);let n=this._nextKeepAlive-new Date().getTime();if(n<0){this._connectionState===Xe.Connected&&this._trySendPingMessage();return}this._pingServerHandle===void 0&&(n<0&&(n=0),this._pingServerHandle=setTimeout(async()=>{this._connectionState===Xe.Connected&&await this._trySendPingMessage()},n))}}serverTimeout(){this.connection.stop(new Error("Server timeout elapsed without receiving a message from the server."))}async _invokeClientMethod(n){let e=n.target.toLowerCase(),i=this._methods[e];if(!i){this._logger.log(w.Warning,`No client method with the name '${e}' found.`),n.invocationId&&(this._logger.log(w.Warning,`No result given for '${e}' method and invocation ID '${n.invocationId}'.`),await this._sendWithProtocol(this._createCompletionMessage(n.invocationId,"Client didn't provide a result.",null)));return}let r=i.slice(),o=!!n.invocationId,s,a,c;for(let l of r)try{let u=s;s=await l.apply(this,n.arguments),o&&s&&u&&(this._logger.log(w.Error,`Multiple results provided for '${e}'. Sending error to server.`),c=this._createCompletionMessage(n.invocationId,"Client provided multiple results.",null)),a=void 0}catch(u){a=u,this._logger.log(w.Error,`A callback for the method '${e}' threw error '${u}'.`)}c?await this._sendWithProtocol(c):o?(a?c=this._createCompletionMessage(n.invocationId,`${a}`,null):s!==void 0?c=this._createCompletionMessage(n.invocationId,null,s):(this._logger.log(w.Warning,`No result given for '${e}' method and invocation ID '${n.invocationId}'.`),c=this._createCompletionMessage(n.invocationId,"Client didn't provide a result.",null)),await this._sendWithProtocol(c)):s&&this._logger.log(w.Error,`Result given for '${e}' method but server is not expecting a result.`)}_connectionClosed(n){this._logger.log(w.Debug,`HubConnection.connectionClosed(${n}) called while in state ${this._connectionState}.`),this._stopDuringStartError=this._stopDuringStartError||n||new Yt("The underlying connection was closed before the hub handshake could complete."),this._handshakeResolver&&this._handshakeResolver(),this._cancelCallbacksWithError(n||new Error("Invocation canceled due to the underlying connection being closed.")),this._cleanupTimeout(),this._cleanupPingTimer(),this._connectionState===Xe.Disconnecting?this._completeClose(n):this._connectionState===Xe.Connected&&this._reconnectPolicy?this._reconnect(n):this._connectionState===Xe.Connected&&this._completeClose(n)}_completeClose(n){if(this._connectionStarted){this._connectionState=Xe.Disconnected,this._connectionStarted=!1,this._messageBuffer&&(this._messageBuffer._dispose(n??new Error("Connection closed.")),this._messageBuffer=void 0),Qe.isBrowser&&window.document.removeEventListener("freeze",this._freezeEventListener);try{this._closedCallbacks.forEach(e=>e.apply(this,[n]))}catch(e){this._logger.log(w.Error,`An onclose callback called with error '${n}' threw error '${e}'.`)}}}async _reconnect(n){let e=Date.now(),i=0,r=n!==void 0?n:new Error("Attempting to reconnect due to a unknown error."),o=this._getNextRetryDelay(i,0,r);if(o===null){this._logger.log(w.Debug,"Connection not reconnecting because the IRetryPolicy returned null on the first reconnect attempt."),this._completeClose(n);return}if(this._connectionState=Xe.Reconnecting,n?this._logger.log(w.Information,`Connection reconnecting because of error '${n}'.`):this._logger.log(w.Information,"Connection reconnecting."),this._reconnectingCallbacks.length!==0){try{this._reconnectingCallbacks.forEach(s=>s.apply(this,[n]))}catch(s){this._logger.log(w.Error,`An onreconnecting callback called with error '${n}' threw error '${s}'.`)}if(this._connectionState!==Xe.Reconnecting){this._logger.log(w.Debug,"Connection left the reconnecting state in onreconnecting callback. Done reconnecting.");return}}for(;o!==null;){if(this._logger.log(w.Information,`Reconnect attempt number ${i+1} will start in ${o} ms.`),await new Promise(s=>{this._reconnectDelayHandle=setTimeout(s,o)}),this._reconnectDelayHandle=void 0,this._connectionState!==Xe.Reconnecting){this._logger.log(w.Debug,"Connection left the reconnecting state during reconnect delay. Done reconnecting.");return}try{if(await this._startInternal(),this._connectionState=Xe.Connected,this._logger.log(w.Information,"HubConnection reconnected successfully."),this._reconnectedCallbacks.length!==0)try{this._reconnectedCallbacks.forEach(s=>s.apply(this,[this.connection.connectionId]))}catch(s){this._logger.log(w.Error,`An onreconnected callback called with connectionId '${this.connection.connectionId}; threw error '${s}'.`)}return}catch(s){if(this._logger.log(w.Information,`Reconnect attempt failed because of error '${s}'.`),this._connectionState!==Xe.Reconnecting){this._logger.log(w.Debug,`Connection moved to the '${this._connectionState}' from the reconnecting state during reconnect attempt. Done reconnecting.`),this._connectionState===Xe.Disconnecting&&this._completeClose();return}i++,r=s instanceof Error?s:new Error(s.toString()),o=this._getNextRetryDelay(i,Date.now()-e,r)}}this._logger.log(w.Information,`Reconnect retries have been exhausted after ${Date.now()-e} ms and ${i} failed attempts. Connection disconnecting.`),this._completeClose()}_getNextRetryDelay(n,e,i){try{return this._reconnectPolicy.nextRetryDelayInMilliseconds({elapsedMilliseconds:e,previousRetryCount:n,retryReason:i})}catch(r){return this._logger.log(w.Error,`IRetryPolicy.nextRetryDelayInMilliseconds(${n}, ${e}) threw error '${r}'.`),null}}_cancelCallbacksWithError(n){let e=this._callbacks;this._callbacks={},Object.keys(e).forEach(i=>{let r=e[i];try{r(null,n)}catch(o){this._logger.log(w.Error,`Stream 'error' callback called with '${n}' threw error: ${Oh(o)}`)}})}_cleanupPingTimer(){this._pingServerHandle&&(clearTimeout(this._pingServerHandle),this._pingServerHandle=void 0)}_cleanupTimeout(){this._timeoutHandle&&clearTimeout(this._timeoutHandle)}_createInvocation(n,e,i,r){if(i)return r.length!==0?{target:n,arguments:e,streamIds:r,type:le.Invocation}:{target:n,arguments:e,type:le.Invocation};{let o=this._invocationId;return this._invocationId++,r.length!==0?{target:n,arguments:e,invocationId:o.toString(),streamIds:r,type:le.Invocation}:{target:n,arguments:e,invocationId:o.toString(),type:le.Invocation}}}_launchStreams(n,e){if(n.length!==0){e||(e=Promise.resolve());for(let i in n)n[i].subscribe({complete:()=>{e=e.then(()=>this._sendWithProtocol(this._createCompletionMessage(i)))},error:r=>{let o;r instanceof Error?o=r.message:r&&r.toString?o=r.toString():o="Unknown error",e=e.then(()=>this._sendWithProtocol(this._createCompletionMessage(i,o)))},next:r=>{e=e.then(()=>this._sendWithProtocol(this._createStreamItemMessage(i,r)))}})}}_replaceStreamingParams(n){let e=[],i=[];for(let r=0;r<n.length;r++){let o=n[r];if(this._isObservable(o)){let s=this._invocationId;this._invocationId++,e[s]=o,i.push(s.toString()),n.splice(r,1)}}return[e,i]}_isObservable(n){return n&&n.subscribe&&typeof n.subscribe=="function"}_createStreamInvocation(n,e,i){let r=this._invocationId;return this._invocationId++,i.length!==0?{target:n,arguments:e,invocationId:r.toString(),streamIds:i,type:le.StreamInvocation}:{target:n,arguments:e,invocationId:r.toString(),type:le.StreamInvocation}}_createCancelInvocation(n){return{invocationId:n,type:le.CancelInvocation}}_createStreamItemMessage(n,e){return{invocationId:n,item:e,type:le.StreamItem}}_createCompletionMessage(n,e,i){return e?{error:e,invocationId:n,type:le.Completion}:{invocationId:n,result:i,type:le.Completion}}_createCloseMessage(){return{type:le.Close}}async _trySendPingMessage(){try{await this._sendMessage(this._cachedPingMessage)}catch{this._cleanupPingTimer()}}};var cH=[0,2e3,1e4,3e4,null],Dd=class{constructor(n){this._retryDelays=n!==void 0?[...n,null]:cH}nextRetryDelayInMilliseconds(n){return this._retryDelays[n.previousRetryCount]}};var Os=(()=>{class t{}return t.Authorization="Authorization",t.Cookie="Cookie",t})();var Vh=class extends rr{constructor(n,e){super(),this._innerClient=n,this._accessTokenFactory=e}async send(n){let e=!0;this._accessTokenFactory&&(!this._accessToken||n.url&&n.url.indexOf("/negotiate?")>0)&&(e=!1,this._accessToken=await this._accessTokenFactory()),this._setAuthorizationHeader(n);let i=await this._innerClient.send(n);return e&&i.statusCode===401&&this._accessTokenFactory?(this._accessToken=await this._accessTokenFactory(),this._setAuthorizationHeader(n),await this._innerClient.send(n)):i}_setAuthorizationHeader(n){n.headers||(n.headers={}),this._accessToken?n.headers[Os.Authorization]=`Bearer ${this._accessToken}`:this._accessTokenFactory&&n.headers[Os.Authorization]&&delete n.headers[Os.Authorization]}getCookieString(n){return this._innerClient.getCookieString(n)}};var Ut=(function(t){return t[t.None=0]="None",t[t.WebSockets=1]="WebSockets",t[t.ServerSentEvents=2]="ServerSentEvents",t[t.LongPolling=4]="LongPolling",t})(Ut||{}),Lt=(function(t){return t[t.Text=1]="Text",t[t.Binary=2]="Binary",t})(Lt||{});var Hh=class{constructor(){this._isAborted=!1,this.onabort=null}abort(){this._isAborted||(this._isAborted=!0,this.onabort&&this.onabort())}get signal(){return this}get aborted(){return this._isAborted}};var Ed=class{get pollAborted(){return this._pollAbort.aborted}constructor(n,e,i){this._httpClient=n,this._logger=e,this._pollAbort=new Hh,this._options=i,this._running=!1,this.onreceive=null,this.onclose=null}async connect(n,e){if(Ye.isRequired(n,"url"),Ye.isRequired(e,"transferFormat"),Ye.isIn(e,Lt,"transferFormat"),this._url=n,this._logger.log(w.Trace,"(LongPolling transport) Connecting."),e===Lt.Binary&&typeof XMLHttpRequest<"u"&&typeof new XMLHttpRequest().responseType!="string")throw new Error("Binary protocols over XmlHttpRequest not implementing advanced features are not supported.");let[i,r]=sr(),o=b({[i]:r},this._options.headers),s={abortSignal:this._pollAbort.signal,headers:o,timeout:1e5,withCredentials:this._options.withCredentials};e===Lt.Binary&&(s.responseType="arraybuffer");let a=`${n}&_=${Date.now()}`;this._logger.log(w.Trace,`(LongPolling transport) polling: ${a}.`);let c=await this._httpClient.get(a,s);c.statusCode!==200?(this._logger.log(w.Error,`(LongPolling transport) Unexpected response code: ${c.statusCode}.`),this._closeError=new zn(c.statusText||"",c.statusCode),this._running=!1):this._running=!0,this._receiving=this._poll(this._url,s)}async _poll(n,e){try{for(;this._running;)try{let i=`${n}&_=${Date.now()}`;this._logger.log(w.Trace,`(LongPolling transport) polling: ${i}.`);let r=await this._httpClient.get(i,e);r.statusCode===204?(this._logger.log(w.Information,"(LongPolling transport) Poll terminated by server."),this._running=!1):r.statusCode!==200?(this._logger.log(w.Error,`(LongPolling transport) Unexpected response code: ${r.statusCode}.`),this._closeError=new zn(r.statusText||"",r.statusCode),this._running=!1):r.content?(this._logger.log(w.Trace,`(LongPolling transport) data received. ${wo(r.content,this._options.logMessageContent)}.`),this.onreceive&&this.onreceive(r.content)):this._logger.log(w.Trace,"(LongPolling transport) Poll timed out, reissuing.")}catch(i){this._running?i instanceof xo?this._logger.log(w.Trace,"(LongPolling transport) Poll timed out, reissuing."):(this._closeError=i,this._running=!1):this._logger.log(w.Trace,`(LongPolling transport) Poll errored after shutdown: ${i.message}`)}}finally{this._logger.log(w.Trace,"(LongPolling transport) Polling complete."),this.pollAborted||this._raiseOnClose()}}async send(n){return this._running?Rh(this._logger,"LongPolling",this._httpClient,this._url,n,this._options):Promise.reject(new Error("Cannot send until the transport is connected"))}async stop(){this._logger.log(w.Trace,"(LongPolling transport) Stopping polling."),this._running=!1,this._pollAbort.abort();try{await this._receiving,this._logger.log(w.Trace,`(LongPolling transport) sending DELETE request to ${this._url}.`);let n={},[e,i]=sr();n[e]=i;let r={headers:b(b({},n),this._options.headers),timeout:this._options.timeout,withCredentials:this._options.withCredentials},o;try{await this._httpClient.delete(this._url,r)}catch(s){o=s}o?o instanceof zn&&(o.statusCode===404?this._logger.log(w.Trace,"(LongPolling transport) A 404 response was returned from sending a DELETE request."):this._logger.log(w.Trace,`(LongPolling transport) Error sending a DELETE request: ${o}`)):this._logger.log(w.Trace,"(LongPolling transport) DELETE request accepted.")}finally{this._logger.log(w.Trace,"(LongPolling transport) Stop finished."),this._raiseOnClose()}}_raiseOnClose(){if(this.onclose){let n="(LongPolling transport) Firing onclose event.";this._closeError&&(n+=" Error: "+this._closeError),this._logger.log(w.Trace,n),this.onclose(this._closeError)}}};var zh=class{constructor(n,e,i,r){this._httpClient=n,this._accessToken=e,this._logger=i,this._options=r,this.onreceive=null,this.onclose=null}async connect(n,e){return Ye.isRequired(n,"url"),Ye.isRequired(e,"transferFormat"),Ye.isIn(e,Lt,"transferFormat"),this._logger.log(w.Trace,"(SSE transport) Connecting."),this._url=n,this._accessToken&&(n+=(n.indexOf("?")<0?"?":"&")+`access_token=${encodeURIComponent(this._accessToken)}`),new Promise((i,r)=>{let o=!1;if(e!==Lt.Text){r(new Error("The Server-Sent Events transport only supports the 'Text' transfer format"));return}let s;if(Qe.isBrowser||Qe.isWebWorker)s=new this._options.EventSource(n,{withCredentials:this._options.withCredentials});else{let a=this._httpClient.getCookieString(n),c={};c.Cookie=a;let[l,u]=sr();c[l]=u,s=new this._options.EventSource(n,{withCredentials:this._options.withCredentials,headers:b(b({},c),this._options.headers)})}try{s.onmessage=a=>{if(this.onreceive)try{this._logger.log(w.Trace,`(SSE transport) data received. ${wo(a.data,this._options.logMessageContent)}.`),this.onreceive(a.data)}catch(c){this._close(c);return}},s.onerror=a=>{o?this._close():r(new Error("EventSource failed to connect. The connection could not be found on the server, either the connection ID is not present on the server, or a proxy is refusing/buffering the connection. If you have multiple servers check that sticky sessions are enabled."))},s.onopen=()=>{this._logger.log(w.Information,`SSE connected to ${this._url}`),this._eventSource=s,o=!0,i()}}catch(a){r(a);return}})}async send(n){return this._eventSource?Rh(this._logger,"SSE",this._httpClient,this._url,n,this._options):Promise.reject(new Error("Cannot send until the transport is connected"))}stop(){return this._close(),Promise.resolve()}_close(n){this._eventSource&&(this._eventSource.close(),this._eventSource=void 0,this.onclose&&this.onclose(n))}};var Uh=class{constructor(n,e,i,r,o,s){this._logger=i,this._accessTokenFactory=e,this._logMessageContent=r,this._webSocketConstructor=o,this._httpClient=n,this.onreceive=null,this.onclose=null,this._headers=s}async connect(n,e){Ye.isRequired(n,"url"),Ye.isRequired(e,"transferFormat"),Ye.isIn(e,Lt,"transferFormat"),this._logger.log(w.Trace,"(WebSockets transport) Connecting.");let i;return this._accessTokenFactory&&(i=await this._accessTokenFactory()),new Promise((r,o)=>{n=n.replace(/^http/,"ws");let s,a=this._httpClient.getCookieString(n),c=!1;if(Qe.isNode||Qe.isReactNative){let l={},[u,m]=sr();l[u]=m,i&&(l[Os.Authorization]=`Bearer ${i}`),a&&(l[Os.Cookie]=a),s=new this._webSocketConstructor(n,void 0,{headers:b(b({},l),this._headers)})}else i&&(n+=(n.indexOf("?")<0?"?":"&")+`access_token=${encodeURIComponent(i)}`);s||(s=new this._webSocketConstructor(n)),e===Lt.Binary&&(s.binaryType="arraybuffer"),s.onopen=l=>{this._logger.log(w.Information,`WebSocket connected to ${n}.`),this._webSocket=s,c=!0,r()},s.onerror=l=>{let u=null;typeof ErrorEvent<"u"&&l instanceof ErrorEvent?u=l.error:u="There was an error with the transport",this._logger.log(w.Information,`(WebSockets transport) ${u}.`)},s.onmessage=l=>{if(this._logger.log(w.Trace,`(WebSockets transport) data received. ${wo(l.data,this._logMessageContent)}.`),this.onreceive)try{this.onreceive(l.data)}catch(u){this._close(u);return}},s.onclose=l=>{if(c)this._close(l);else{let u=null;typeof ErrorEvent<"u"&&l instanceof ErrorEvent?u=l.error:u="WebSocket failed to connect. The connection could not be found on the server, either the endpoint may not be a SignalR endpoint, the connection ID is not present on the server, or there is a proxy blocking WebSockets. If you have multiple servers check that sticky sessions are enabled.",o(new Error(u))}}})}send(n){return this._webSocket&&this._webSocket.readyState===this._webSocketConstructor.OPEN?(this._logger.log(w.Trace,`(WebSockets transport) sending data. ${wo(n,this._logMessageContent)}.`),this._webSocket.send(n),Promise.resolve()):Promise.reject("WebSocket is not in the OPEN state")}stop(){return this._webSocket&&this._close(void 0),Promise.resolve()}_close(n){this._webSocket&&(this._webSocket.onclose=()=>{},this._webSocket.onmessage=()=>{},this._webSocket.onerror=()=>{},this._webSocket.close(),this._webSocket=void 0),this._logger.log(w.Trace,"(WebSockets transport) socket closed."),this.onclose&&(this._isCloseEvent(n)&&(n.wasClean===!1||n.code!==1e3)?this.onclose(new Error(`WebSocket closed with status code: ${n.code} (${n.reason||"no reason given"}).`)):n instanceof Error?this.onclose(n):this.onclose())}_isCloseEvent(n){return n&&typeof n.wasClean=="boolean"&&typeof n.code=="number"}};var qk=100,$h=class{constructor(n,e={}){if(this._stopPromiseResolver=()=>{},this.features={},this._negotiateVersion=1,Ye.isRequired(n,"url"),this._logger=$k(e.logger),this.baseUrl=this._resolveUrl(n),e=e||{},e.logMessageContent=e.logMessageContent===void 0?!1:e.logMessageContent,typeof e.withCredentials=="boolean"||e.withCredentials===void 0)e.withCredentials=e.withCredentials===void 0?!0:e.withCredentials;else throw new Error("withCredentials option was not a 'boolean' or 'undefined' value");e.timeout=e.timeout===void 0?100*1e3:e.timeout;let i=null,r=null;if(Qe.isNode&&typeof Ns<"u"){let o=typeof __webpack_require__=="function"?__non_webpack_require__:Ns;i=o("ws"),r=o("eventsource")}!Qe.isNode&&typeof WebSocket<"u"&&!e.WebSocket?e.WebSocket=WebSocket:Qe.isNode&&!e.WebSocket&&i&&(e.WebSocket=i),!Qe.isNode&&typeof EventSource<"u"&&!e.EventSource?e.EventSource=EventSource:Qe.isNode&&!e.EventSource&&typeof r<"u"&&(e.EventSource=r),this._httpClient=new Vh(e.httpClient||new Fh(this._logger),e.accessTokenFactory),this._connectionState="Disconnected",this._connectionStarted=!1,this._options=e,this.onreceive=null,this.onclose=null}async start(n){if(n=n||Lt.Binary,Ye.isIn(n,Lt,"transferFormat"),this._logger.log(w.Debug,`Starting connection with transfer format '${Lt[n]}'.`),this._connectionState!=="Disconnected")return Promise.reject(new Error("Cannot start an HttpConnection that is not in the 'Disconnected' state."));if(this._connectionState="Connecting",this._startInternalPromise=this._startInternal(n),await this._startInternalPromise,this._connectionState==="Disconnecting"){let e="Failed to start the HttpConnection before stop() was called.";return this._logger.log(w.Error,e),await this._stopPromise,Promise.reject(new Yt(e))}else if(this._connectionState!=="Connected"){let e="HttpConnection.startInternal completed gracefully but didn't enter the connection into the connected state!";return this._logger.log(w.Error,e),Promise.reject(new Yt(e))}this._connectionStarted=!0}send(n){return this._connectionState!=="Connected"?Promise.reject(new Error("Cannot send data if the connection is not in the 'Connected' State.")):(this._sendQueue||(this._sendQueue=new Ry(this.transport)),this._sendQueue.send(n))}async stop(n){if(this._connectionState==="Disconnected")return this._logger.log(w.Debug,`Call to HttpConnection.stop(${n}) ignored because the connection is already in the disconnected state.`),Promise.resolve();if(this._connectionState==="Disconnecting")return this._logger.log(w.Debug,`Call to HttpConnection.stop(${n}) ignored because the connection is already in the disconnecting state.`),this._stopPromise;this._connectionState="Disconnecting",this._stopPromise=new Promise(e=>{this._stopPromiseResolver=e}),await this._stopInternal(n),await this._stopPromise}async _stopInternal(n){this._stopError=n;try{await this._startInternalPromise}catch{}if(this.transport){try{await this.transport.stop()}catch(e){this._logger.log(w.Error,`HttpConnection.transport.stop() threw error '${e}'.`),this._stopConnection()}this.transport=void 0}else this._logger.log(w.Debug,"HttpConnection.transport is undefined in HttpConnection.stop() because start() failed.")}async _startInternal(n){let e=this.baseUrl;this._accessTokenFactory=this._options.accessTokenFactory,this._httpClient._accessTokenFactory=this._accessTokenFactory;try{if(this._options.skipNegotiation)if(this._options.transport===Ut.WebSockets)this.transport=this._constructTransport(Ut.WebSockets),await this._startTransport(e,n);else throw new Error("Negotiation can only be skipped when using the WebSocket transport directly.");else{let i=null,r=0;do{if(i=await this._getNegotiationResponse(e),this._connectionState==="Disconnecting"||this._connectionState==="Disconnected")throw new Yt("The connection was stopped during negotiation.");if(i.error)throw new Error(i.error);if(i.ProtocolVersion)throw new Error("Detected a connection attempt to an ASP.NET SignalR Server. This client only supports connecting to an ASP.NET Core SignalR Server. See https://aka.ms/signalr-core-differences for details.");if(i.url&&(e=i.url),i.accessToken){let o=i.accessToken;this._accessTokenFactory=()=>o,this._httpClient._accessToken=o,this._httpClient._accessTokenFactory=void 0}r++}while(i.url&&r<qk);if(r===qk&&i.url)throw new Error("Negotiate redirection limit exceeded.");await this._createTransport(e,this._options.transport,i,n)}this.transport instanceof Ed&&(this.features.inherentKeepAlive=!0),this._connectionState==="Connecting"&&(this._logger.log(w.Debug,"The HttpConnection connected successfully."),this._connectionState="Connected")}catch(i){return this._logger.log(w.Error,"Failed to start the connection: "+i),this._connectionState="Disconnected",this.transport=void 0,this._stopPromiseResolver(),Promise.reject(i)}}async _getNegotiationResponse(n){let e={},[i,r]=sr();e[i]=r;let o=this._resolveNegotiateUrl(n);this._logger.log(w.Debug,`Sending negotiation request: ${o}.`);try{let s=await this._httpClient.post(o,{content:"",headers:b(b({},e),this._options.headers),timeout:this._options.timeout,withCredentials:this._options.withCredentials});if(s.statusCode!==200)return Promise.reject(new Error(`Unexpected status code returned from negotiate '${s.statusCode}'`));let a=JSON.parse(s.content);return(!a.negotiateVersion||a.negotiateVersion<1)&&(a.connectionToken=a.connectionId),a.useStatefulReconnect&&this._options._useStatefulReconnect!==!0?Promise.reject(new xd("Client didn't negotiate Stateful Reconnect but the server did.")):a}catch(s){let a="Failed to complete negotiation with the server: "+s;return s instanceof zn&&s.statusCode===404&&(a=a+" Either this is not a SignalR endpoint or there is a proxy blocking the connection."),this._logger.log(w.Error,a),Promise.reject(new xd(a))}}_createConnectUrl(n,e){return e?n+(n.indexOf("?")===-1?"?":"&")+`id=${e}`:n}async _createTransport(n,e,i,r){let o=this._createConnectUrl(n,i.connectionToken);if(this._isITransport(e)){this._logger.log(w.Debug,"Connection was provided an instance of ITransport, using that directly."),this.transport=e,await this._startTransport(o,r),this.connectionId=i.connectionId;return}let s=[],a=i.availableTransports||[],c=i;for(let l of a){let u=this._resolveTransportOrError(l,e,r,c?.useStatefulReconnect===!0);if(u instanceof Error)s.push(`${l.transport} failed:`),s.push(u);else if(this._isITransport(u)){if(this.transport=u,!c){try{c=await this._getNegotiationResponse(n)}catch(m){return Promise.reject(m)}o=this._createConnectUrl(n,c.connectionToken)}try{await this._startTransport(o,r),this.connectionId=c.connectionId;return}catch(m){if(this._logger.log(w.Error,`Failed to start the transport '${l.transport}': ${m}`),c=void 0,s.push(new Mh(`${l.transport} failed: ${m}`,Ut[l.transport])),this._connectionState!=="Connecting"){let g="Failed to select transport before stop() was called.";return this._logger.log(w.Debug,g),Promise.reject(new Yt(g))}}}}return s.length>0?Promise.reject(new Th(`Unable to connect to the server with any of the available transports. ${s.join(" ")}`,s)):Promise.reject(new Error("None of the transports supported by the client are supported by the server."))}_constructTransport(n){switch(n){case Ut.WebSockets:if(!this._options.WebSocket)throw new Error("'WebSocket' is not supported in your environment.");return new Uh(this._httpClient,this._accessTokenFactory,this._logger,this._options.logMessageContent,this._options.WebSocket,this._options.headers||{});case Ut.ServerSentEvents:if(!this._options.EventSource)throw new Error("'EventSource' is not supported in your environment.");return new zh(this._httpClient,this._httpClient._accessToken,this._logger,this._options);case Ut.LongPolling:return new Ed(this._httpClient,this._logger,this._options);default:throw new Error(`Unknown transport: ${n}.`)}}_startTransport(n,e){return this.transport.onreceive=this.onreceive,this.features.reconnect?this.transport.onclose=async i=>{let r=!1;if(this.features.reconnect)try{this.features.disconnected(),await this.transport.connect(n,e),await this.features.resend()}catch{r=!0}else{this._stopConnection(i);return}r&&this._stopConnection(i)}:this.transport.onclose=i=>this._stopConnection(i),this.transport.connect(n,e)}_resolveTransportOrError(n,e,i,r){let o=Ut[n.transport];if(o==null)return this._logger.log(w.Debug,`Skipping transport '${n.transport}' because it is not supported by this client.`),new Error(`Skipping transport '${n.transport}' because it is not supported by this client.`);if(lH(e,o))if(n.transferFormats.map(a=>Lt[a]).indexOf(i)>=0){if(o===Ut.WebSockets&&!this._options.WebSocket||o===Ut.ServerSentEvents&&!this._options.EventSource)return this._logger.log(w.Debug,`Skipping transport '${Ut[o]}' because it is not supported in your environment.'`),new Ih(`'${Ut[o]}' is not supported in your environment.`,o);this._logger.log(w.Debug,`Selecting transport '${Ut[o]}'.`);try{return this.features.reconnect=o===Ut.WebSockets?r:void 0,this._constructTransport(o)}catch(a){return a}}else return this._logger.log(w.Debug,`Skipping transport '${Ut[o]}' because it does not support the requested transfer format '${Lt[i]}'.`),new Error(`'${Ut[o]}' does not support ${Lt[i]}.`);else return this._logger.log(w.Debug,`Skipping transport '${Ut[o]}' because it was disabled by the client.`),new kh(`'${Ut[o]}' is disabled by the client.`,o)}_isITransport(n){return n&&typeof n=="object"&&"connect"in n}_stopConnection(n){if(this._logger.log(w.Debug,`HttpConnection.stopConnection(${n}) called while in state ${this._connectionState}.`),this.transport=void 0,n=this._stopError||n,this._stopError=void 0,this._connectionState==="Disconnected"){this._logger.log(w.Debug,`Call to HttpConnection.stopConnection(${n}) was ignored because the connection is already in the disconnected state.`);return}if(this._connectionState==="Connecting")throw this._logger.log(w.Warning,`Call to HttpConnection.stopConnection(${n}) was ignored because the connection is still in the connecting state.`),new Error(`HttpConnection.stopConnection(${n}) was called while the connection is still in the connecting state.`);if(this._connectionState==="Disconnecting"&&this._stopPromiseResolver(),n?this._logger.log(w.Error,`Connection disconnected with error '${n}'.`):this._logger.log(w.Information,"Connection disconnected."),this._sendQueue&&(this._sendQueue.stop().catch(e=>{this._logger.log(w.Error,`TransportSendQueue.stop() threw error '${e}'.`)}),this._sendQueue=void 0),this.connectionId=void 0,this._connectionState="Disconnected",this._connectionStarted){this._connectionStarted=!1;try{this.onclose&&this.onclose(n)}catch(e){this._logger.log(w.Error,`HttpConnection.onclose(${n}) threw error '${e}'.`)}}}_resolveUrl(n){if(n.lastIndexOf("https://",0)===0||n.lastIndexOf("http://",0)===0)return n;if(!Qe.isBrowser)throw new Error(`Cannot resolve '${n}'.`);let e=window.document.createElement("a");return e.href=n,this._logger.log(w.Information,`Normalizing '${n}' to '${e.href}'.`),e.href}_resolveNegotiateUrl(n){let e=new URL(n);e.pathname.endsWith("/")?e.pathname+="negotiate":e.pathname+="/negotiate";let i=new URLSearchParams(e.searchParams);return i.has("negotiateVersion")||i.append("negotiateVersion",this._negotiateVersion.toString()),i.has("useStatefulReconnect")?i.get("useStatefulReconnect")==="true"&&(this._options._useStatefulReconnect=!0):this._options._useStatefulReconnect===!0&&i.append("useStatefulReconnect","true"),e.search=i.toString(),e.toString()}};function lH(t,n){return!t||(n&t)!==0}var Ry=class t{constructor(n){this._transport=n,this._buffer=[],this._executing=!0,this._sendBufferedData=new ic,this._transportResult=new ic,this._sendLoopPromise=this._sendLoop()}send(n){return this._bufferData(n),this._transportResult||(this._transportResult=new ic),this._transportResult.promise}stop(){return this._executing=!1,this._sendBufferedData.resolve(),this._sendLoopPromise}_bufferData(n){if(this._buffer.length&&typeof this._buffer[0]!=typeof n)throw new Error(`Expected data to be of type ${typeof this._buffer} but was of type ${typeof n}`);this._buffer.push(n),this._sendBufferedData.resolve()}async _sendLoop(){for(;;){if(await this._sendBufferedData.promise,!this._executing){this._transportResult&&this._transportResult.reject("Connection stopped.");break}this._sendBufferedData=new ic;let n=this._transportResult;this._transportResult=void 0;let e=typeof this._buffer[0]=="string"?this._buffer.join(""):t._concatBuffers(this._buffer);this._buffer.length=0;try{await this._transport.send(e),n.resolve()}catch(i){n.reject(i)}}}static _concatBuffers(n){let e=n.map(o=>o.byteLength).reduce((o,s)=>o+s),i=new Uint8Array(e),r=0;for(let o of n)i.set(new Uint8Array(o),r),r+=o.byteLength;return i.buffer}},ic=class{constructor(){this.promise=new Promise((n,e)=>[this._resolver,this._rejecter]=[n,e])}resolve(){this._resolver()}reject(n){this._rejecter(n)}};var dH="json",Gh=class{constructor(){this.name=dH,this.version=2,this.transferFormat=Lt.Text}parseMessages(n,e){if(typeof n!="string")throw new Error("Invalid input for JSON hub protocol. Expected a string.");if(!n)return[];e===null&&(e=or.instance);let i=Un.parse(n),r=[];for(let o of i){let s=JSON.parse(o);if(typeof s.type!="number")throw new Error("Invalid payload.");switch(s.type){case le.Invocation:this._isInvocationMessage(s);break;case le.StreamItem:this._isStreamItemMessage(s);break;case le.Completion:this._isCompletionMessage(s);break;case le.Ping:break;case le.Close:break;case le.Ack:this._isAckMessage(s);break;case le.Sequence:this._isSequenceMessage(s);break;default:e.log(w.Information,"Unknown message type '"+s.type+"' ignored.");continue}r.push(s)}return r}writeMessage(n){return Un.write(JSON.stringify(n))}_isInvocationMessage(n){this._assertNotEmptyString(n.target,"Invalid payload for Invocation message."),n.invocationId!==void 0&&this._assertNotEmptyString(n.invocationId,"Invalid payload for Invocation message.")}_isStreamItemMessage(n){if(this._assertNotEmptyString(n.invocationId,"Invalid payload for StreamItem message."),n.item===void 0)throw new Error("Invalid payload for StreamItem message.")}_isCompletionMessage(n){if(n.result&&n.error)throw new Error("Invalid payload for Completion message.");!n.result&&n.error&&this._assertNotEmptyString(n.error,"Invalid payload for Completion message."),this._assertNotEmptyString(n.invocationId,"Invalid payload for Completion message.")}_isAckMessage(n){if(typeof n.sequenceId!="number")throw new Error("Invalid SequenceId for Ack message.")}_isSequenceMessage(n){if(typeof n.sequenceId!="number")throw new Error("Invalid SequenceId for Sequence message.")}_assertNotEmptyString(n,e){if(typeof n!="string"||n==="")throw new Error(e)}};var uH={trace:w.Trace,debug:w.Debug,info:w.Information,information:w.Information,warn:w.Warning,warning:w.Warning,error:w.Error,critical:w.Critical,none:w.None};function mH(t){let n=uH[t.toLowerCase()];if(typeof n<"u")return n;throw new Error(`Unknown log level: ${t}`)}var Sd=class{configureLogging(n){if(Ye.isRequired(n,"logging"),fH(n))this.logger=n;else if(typeof n=="string"){let e=mH(n);this.logger=new Rs(e)}else this.logger=new Rs(n);return this}withUrl(n,e){return Ye.isRequired(n,"url"),Ye.isNotEmpty(n,"url"),this.url=n,typeof e=="object"?this.httpConnectionOptions=b(b({},this.httpConnectionOptions),e):this.httpConnectionOptions=J(b({},this.httpConnectionOptions),{transport:e}),this}withHubProtocol(n){return Ye.isRequired(n,"protocol"),this.protocol=n,this}withAutomaticReconnect(n){if(this.reconnectPolicy)throw new Error("A reconnectPolicy has already been set.");return n?Array.isArray(n)?this.reconnectPolicy=new Dd(n):this.reconnectPolicy=n:this.reconnectPolicy=new Dd,this}withServerTimeout(n){return Ye.isRequired(n,"milliseconds"),this._serverTimeoutInMilliseconds=n,this}withKeepAliveInterval(n){return Ye.isRequired(n,"milliseconds"),this._keepAliveIntervalInMilliseconds=n,this}withStatefulReconnect(n){return this.httpConnectionOptions===void 0&&(this.httpConnectionOptions={}),this.httpConnectionOptions._useStatefulReconnect=!0,this._statefulReconnectBufferSize=n?.bufferSize,this}build(){let n=this.httpConnectionOptions||{};if(n.logger===void 0&&(n.logger=this.logger),!this.url)throw new Error("The 'HubConnectionBuilder.withUrl' method must be called before building the connection.");let e=new $h(this.url,n);return wd.create(e,this.logger||or.instance,this.protocol||new Gh,this.reconnectPolicy,this._serverTimeoutInMilliseconds,this._keepAliveIntervalInMilliseconds,this._statefulReconnectBufferSize)}};function fH(t){return t.log!==void 0}var ar=class t{hubUrl=Kt.hubUrl;hubConnection;orderSignal=N(null);createHubConnection(){this.hubConnection||(this.hubConnection=new Sd().withUrl(this.hubUrl,{accessTokenFactory:()=>localStorage.getItem("token")??"",withCredentials:!0}).withAutomaticReconnect().build(),this.hubConnection?.start().then(()=>console.log("SignalR connected")).catch(n=>console.log(n)),this.hubConnection.on("OrderCompleteNotification",n=>{this.orderSignal.set(n)}))}stopHubConnection(){this.hubConnection?.state===Xe.Connected&&this.hubConnection?.stop().catch(n=>console.log(n)),this.hubConnection=void 0}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var Zt=class t{baseUrl=Kt.apiUrl;http=d(dn);signalrServ=d(ar);currentUser=N(null);login(n){let e=new Fn().set("useCookies","true");return this.http.post(this.baseUrl+"account/login",n,{params:e}).pipe(ct(i=>{localStorage.setItem("token",i.token),this.currentUser.set(i),this.signalrServ.createHubConnection()}))}register(n){return this.http.post(this.baseUrl+"account/register",n)}getUserInfo(){let n=localStorage.getItem("token"),e={Authorization:`Bearer ${n}`};return this.http.get(this.baseUrl+"account/user-info",{headers:{Authorization:`Bearer ${n}`}}).pipe(ct(i=>this.currentUser.set(i)))}updateAddress(n){let e=localStorage.getItem("token");return this.http.post(this.baseUrl+"account/address",n,{headers:{Authorization:`Bearer ${e}`}})}logout(){localStorage.removeItem("token"),this.currentUser.set(null),this.signalrServ.stopHubConnection()}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var rc=class t{baseUrl=Kt.apiUrl;cartService=d(it);accountService=d(Zt);http=d(dn);stripePromise;elements;addressElement;paymentElement;constructor(){this.stripePromise=zk(Kt.stripePublicKey)}getStripeInstance(){return this.stripePromise}async initializeElements(){if(!this.elements){let n=await this.getStripeInstance();if(n){let e=await Bo(this.createOrUpdatePaymentIntent());this.elements=n.elements({clientSecret:e.clientSecret,appearance:{labels:"floating"}})}else throw new Error("Stripe has not been loaded")}return this.elements}async createPaymentElement(){if(!this.paymentElement){let n=await this.initializeElements();if(n)this.paymentElement=n.create("payment");else throw new Error("Elements instance has not been initialized")}return this.paymentElement}async createAddressElement(){if(!this.addressElement){let n=await this.initializeElements();if(n){let e=this.accountService.currentUser(),i={};e&&(i.name=e.firstName+" "+e.lastName),e?.address&&(i.address={line1:e.address.line1,line2:e.address.line2,city:e.address.city,state:e.address.state,country:e.address.country,postal_code:e.address.postalCode});let r={mode:"shipping",defaultValues:i};this.addressElement=n.create("address",r)}else throw new Error("Element Instance not loaded")}return this.addressElement}async createConfirmationToken(){let n=await this.getStripeInstance(),e=await this.initializeElements(),i=await e.submit();if(i.error)throw new Error(i.error.message);if(n)return await n.createConfirmationToken({elements:e});throw new Error("Stripe not available")}async confirmPayment(n){let e=await this.getStripeInstance(),r=await(await this.initializeElements()).submit();if(r.error)throw new Error(r.error.message);let o=this.cartService.cart()?.clientSecret;if(e&&o)return await e.confirmPayment({clientSecret:o,confirmParams:{confirmation_token:n.id},redirect:"if_required"});throw new Error("Unable to load Stripe")}createOrUpdatePaymentIntent(){if(!this.cartService.cart())throw new Error("Problem with cart");let e=localStorage.getItem("token");return this.http.post(this.baseUrl+"payments",{},{headers:{Authorization:`Bearer ${e}`},withCredentials:!0}).pipe(ee(i=>(this.cartService.setCart(i),i)))}disposeElements(){this.elements=void 0,this.addressElement=void 0,this.paymentElement=void 0}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};function hH(t,n){if(t&1){let e=dt();f(0,"div",1)(1,"button",2),R("click",function(){Me(e);let r=S();return Te(r.action())}),v(2),h()()}if(t&2){let e=S();p(2),De(" ",e.data.action," ")}}var pH=["label"];function gH(t,n){}var _H=Math.pow(2,31)-1,Id=class{_overlayRef;instance;containerInstance;_afterDismissed=new I;_afterOpened=new I;_onAction=new I;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,_H))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},Yk=new y("MatSnackBarData"),oc=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},vH=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),bH=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),yH=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),CH=(()=>{class t{snackBarRef=d(Id);data=d(Yk);constructor(){}action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(f(0,"div",0),v(1),h(),F(2,hH,3,1,"div",1)),i&2&&(p(),De(" ",r.data.message,`
`),p(),L(r.hasAction?2:-1))},dependencies:[pt,vH,bH,yH],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),Oy="_mat-snack-bar-enter",Ny="_mat-snack-bar-exit",xH=(()=>{class t extends mo{_ngZone=d(j);_elementRef=d(P);_changeDetectorRef=d(ue);_platform=d(Ce);_animationsDisabled=ke();snackBarConfig=d(oc);_document=d(K);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=d(X);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new I;_onExit=new I;_onEnter=new I;_animationState="void";_live;_label;_role;_liveElementId=d(Ie).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===Ny?this._completeExit():e===Oy&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?bt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(Oy)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(Oy)},200)))}exit(){return this._destroyed?Q(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?bt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(Ny)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(Ny),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(s=>e.classList.add(s)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");this._trackedModals.add(o),s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&je(nr,7)(pH,7),i&2){let o;U(o=$())&&(r._portalOutlet=o.first),U(o=$())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&R("animationend",function(s){return r.onAnimationEnd(s.animationName)})("animationcancel",function(s){return r.onAnimationEnd(s.animationName)}),i&2&&z("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[_e],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(f(0,"div",1)(1,"div",2,0)(3,"div",3),Be(4,gH,0,0,"ng-template",4),h(),M(5,"div"),h()()),i&2&&(p(5),Y("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[nr],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--mat-snack-bar-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-snack-bar-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-snack-bar-container-color, var(--mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--mat-snack-bar-supporting-text-font, var(--mat-sys-body-medium-font));
  font-size: var(--mat-snack-bar-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-snack-bar-supporting-text-weight, var(--mat-sys-body-medium-weight));
  line-height: var(--mat-snack-bar-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--mat-snack-bar-button-color, var(--mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --mat-button-text-state-layer-color: currentColor;
  --mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2})}return t})(),wH=new y("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new oc}),Zk=(()=>{class t{_live=d(Vl);_injector=d(X);_breakpointObserver=d(Bl);_parentSnackBar=d(t,{optional:!0,skipSelf:!0});_defaultConfig=d(wH);_animationsDisabled=ke();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=CH;snackBarContainerComponent=xH;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}constructor(){}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=b(b({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=X.create({parent:r||this._injector,providers:[{provide:oc,useValue:i}]}),s=new wi(this.snackBarContainerComponent,i.viewContainerRef,o),a=e.attach(s);return a.instance.snackBarConfig=i,a.instance}_attach(e,i){let r=b(b(b({},new oc),this._defaultConfig),i),o=this._createOverlay(r),s=this._attachSnackBarContainer(o,r),a=new Id(s,o);if(e instanceof mt){let c=new Vn(e,null,{$implicit:r.data,snackBarRef:a});a.instance=s.attachTemplatePortal(c)}else{let c=this._createInjector(r,a),l=new wi(e,void 0,c),u=s.attachComponentPortal(l);a.instance=u.instance}return this._breakpointObserver.observe(BS.HandsetPortrait).pipe(pe(o.detachments())).subscribe(c=>{o.overlayElement.classList.toggle(this.handsetCssClass,c.matches)}),r.announcementMessage&&s._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(a,r),this._openedSnackBarRef=a,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new Di;i.direction=e.direction;let r=fo(this._injector),o=e.direction==="rtl",s=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,a=!s&&e.horizontalPosition!=="center";return s?r.left("0"):a?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,Ei(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return X.create({parent:r||this._injector,providers:[{provide:Id,useValue:i},{provide:Yk,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Do=class t{snackbarS=d(Zk);error(n){this.snackbarS.open(n,"Close",{duration:5e3,panelClass:["snack-error"]})}success(n){this.snackbarS.open(n,"Close",{duration:5e3,panelClass:["snack-success"]})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var DH=["mat-internal-form-field",""],EH=["*"],Wh=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&z("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},attrs:DH,ngContentSelectors:EH,decls:1,vars:0,template:function(i,r){i&1&&(Se(),se(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2,changeDetection:0})}return t})();var SH=["input"],IH=["label"],kH=["*"],Py={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},MH=new y("mat-checkbox-default-options",{providedIn:"root",factory:()=>Py}),mn=(function(t){return t[t.Init=0]="Init",t[t.Checked=1]="Checked",t[t.Unchecked=2]="Unchecked",t[t.Indeterminate=3]="Indeterminate",t})(mn||{}),Fy=class{source;checked},Ly=(()=>{class t{_elementRef=d(P);_changeDetectorRef=d(ue);_ngZone=d(j);_animationsDisabled=ke();_options=d(MH,{optional:!0});focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(e){let i=new Fy;return i.source=this,i.checked=e,i}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new H;indeterminateChange=new H;value;disableRipple=!1;_inputElement;_labelElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=mn.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){d(qe).load(Tn);let e=d(new Jn("tabindex"),{optional:!0});this._options=this._options||Py,this.color=this._options.color||Py.color,this.tabIndex=e==null?0:parseInt(e)||0,this.id=this._uniqueId=d(Ie).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(e){e.required&&this._validatorChangeFn()}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(e){e!=this.checked&&(this._checked=e,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(e){e!==this.disabled&&(this._disabled=e,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(e){let i=e!=this._indeterminate();this._indeterminate.set(e),i&&(e?this._transitionCheckState(mn.Indeterminate):this._transitionCheckState(this.checked?mn.Checked:mn.Unchecked),this.indeterminateChange.emit(e)),this._syncIndeterminate(e)}_indeterminate=N(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(e){this.checked=!!e}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorChangeFn=e}_transitionCheckState(e){let i=this._currentCheckState,r=this._getAnimationTargetElement();if(!(i===e||!r)&&(this._currentAnimationClass&&r.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(i,e),this._currentCheckState=e,this._currentAnimationClass.length>0)){r.classList.add(this._currentAnimationClass);let o=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{r.classList.remove(o)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let e=this._options?.clickAction;!this.disabled&&e!=="noop"?(this.indeterminate&&e!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?mn.Checked:mn.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&e==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(e){e.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(e,i){if(this._animationsDisabled)return"";switch(e){case mn.Init:if(i===mn.Checked)return this._animationClasses.uncheckedToChecked;if(i==mn.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case mn.Unchecked:return i===mn.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case mn.Checked:return i===mn.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case mn.Indeterminate:return i===mn.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(e){let i=this._inputElement;i&&(i.nativeElement.indeterminate=e)}_onInputClick(){this._handleInputClick()}_onTouchTargetClick(){this._handleInputClick(),this.disabled||this._inputElement.nativeElement.focus()}_preventBubblingFromLabel(e){e.target&&this._labelElement.nativeElement.contains(e.target)&&e.stopPropagation()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-checkbox"]],viewQuery:function(i,r){if(i&1&&je(SH,5)(IH,5),i&2){let o;U(o=$())&&(r._inputElement=o.first),U(o=$())&&(r._labelElement=o.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(i,r){i&2&&(ft("id",r.id),Y("tabindex",null)("aria-label",null)("aria-labelledby",null),It(r.color?"mat-"+r.color:"mat-accent"),z("_mat-animation-noopable",r._animationsDisabled)("mdc-checkbox--disabled",r.disabled)("mat-mdc-checkbox-disabled",r.disabled)("mat-mdc-checkbox-checked",r.checked)("mat-mdc-checkbox-disabled-interactive",r.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",V],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",V],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",V],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?void 0:ut(e)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",V],checked:[2,"checked","checked",V],disabled:[2,"disabled","disabled",V],indeterminate:[2,"indeterminate","indeterminate",V]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[Re([{provide:ki,useExisting:Rt(()=>t),multi:!0},{provide:_o,useExisting:t,multi:!0}]),$e],ngContentSelectors:kH,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target",3,"click"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-label",3,"for"]],template:function(i,r){if(i&1&&(Se(),f(0,"div",3),R("click",function(s){return r._preventBubblingFromLabel(s)}),f(1,"div",4,0)(3,"div",5),R("click",function(){return r._onTouchTargetClick()}),h(),f(4,"input",6,1),R("blur",function(){return r._onBlur()})("click",function(){return r._onInputClick()})("change",function(s){return r._onInteractionEvent(s)}),h(),M(6,"div",7),f(7,"div",8),Wt(),f(8,"svg",9),M(9,"path",10),h(),$i(),M(10,"div",11),h(),M(11,"div",12),h(),f(12,"label",13,2),se(14),h()()),i&2){let o=Ve(2);D("labelPosition",r.labelPosition),p(4),z("mdc-checkbox--selected",r.checked),D("checked",r.checked)("indeterminate",r.indeterminate)("disabled",r.disabled&&!r.disabledInteractive)("id",r.inputId)("required",r.required)("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex),Y("aria-label",r.ariaLabel||null)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-checked",r.indeterminate?"mixed":null)("aria-controls",r.ariaControls)("aria-disabled",r.disabled&&r.disabledInteractive?!0:null)("aria-expanded",r.ariaExpanded)("aria-owns",r.ariaOwns)("name",r.name)("value",r.value),p(7),D("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),p(),D("for",r.inputId)}},dependencies:[tr,Wh],styles:[`.mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}
.mdc-checkbox:hover > .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:hover > .mat-mdc-checkbox-ripple > .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-pressed-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-pressed-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-hover-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-hover-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-focus-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-focus-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-pressed-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-pressed-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-checkbox-state-layer-size, 40px);
  height: var(--mat-checkbox-state-layer-size, 40px);
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}

.mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}

.mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--mat-checkbox-unselected-icon-color, var(--mat-sys-on-surface-variant));
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}

.mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-hover-icon-color, var(--mat-sys-on-surface));
  background-color: transparent;
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-focus-icon-color, var(--mat-sys-on-surface));
}

.mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}

.mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}

.mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}

.mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}

.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}

.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}

@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-checkbox {
  display: inline-block;
  position: relative;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-checkbox label {
  cursor: pointer;
}
.mat-mdc-checkbox .mat-internal-form-field {
  color: var(--mat-checkbox-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-checkbox-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-checkbox-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-checkbox-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-checkbox-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-checkbox-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive input {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
  cursor: default;
  color: var(--mat-checkbox-disabled-label-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
    color: GrayText;
  }
}
.mat-mdc-checkbox label:empty {
  display: none;
}
.mat-mdc-checkbox .mdc-checkbox__ripple {
  opacity: 0;
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple,
.mdc-checkbox__ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-checkbox .mat-mdc-checkbox-ripple:not(:empty),
.mdc-checkbox__ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-mdc-checkbox-ripple .mat-ripple-element {
  opacity: 0.1;
}

.mat-mdc-checkbox-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-checkbox-touch-target-size, 48px);
  width: var(--mat-checkbox-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--mat-checkbox-touch-target-display, block);
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple::before {
  border-radius: 50%;
}

.mdc-checkbox__native-control:focus-visible ~ .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return t})(),Qk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[Ly,ye]})}return t})();var qh=class t{baseUrl=Kt.apiUrl;http=d(dn);deliveryMethods=[];getDeliveryMethods(){return this.deliveryMethods.length>0?Q(this.deliveryMethods):this.http.get(this.baseUrl+"payments/delivery-methods").pipe(ee(n=>(this.deliveryMethods=n.sort((e,i)=>i.price-e.price),n)))}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var AH=["input"],RH=["formField"],OH=["*"],Yh=class{source;value;constructor(n,e){this.source=n,this.value=e}},NH={provide:ki,useExisting:Rt(()=>By),multi:!0},Xk=new y("MatRadioGroup"),PH=new y("mat-radio-default-options",{providedIn:"root",factory:()=>({color:"accent",disabledInteractive:!1})}),By=(()=>{class t{_changeDetector=d(ue);_value=null;_name=d(Ie).getId("mat-radio-group-");_selected=null;_isInitialized=!1;_labelPosition="after";_disabled=!1;_required=!1;_buttonChanges;_controlValueAccessorChangeFn=()=>{};onTouched=()=>{};change=new H;_radios;color;get name(){return this._name}set name(e){this._name=e,this._updateRadioButtonNames()}get labelPosition(){return this._labelPosition}set labelPosition(e){this._labelPosition=e==="before"?"before":"after",this._markRadiosForCheck()}get value(){return this._value}set value(e){this._value!==e&&(this._value=e,this._updateSelectedRadioFromValue(),this._checkSelectedRadioButton())}_checkSelectedRadioButton(){this._selected&&!this._selected.checked&&(this._selected.checked=!0)}get selected(){return this._selected}set selected(e){this._selected=e,this.value=e?e.value:null,this._checkSelectedRadioButton()}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._markRadiosForCheck()}get required(){return this._required}set required(e){this._required=e,this._markRadiosForCheck()}get disabledInteractive(){return this._disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e,this._markRadiosForCheck()}_disabledInteractive=!1;constructor(){}ngAfterContentInit(){this._isInitialized=!0,this._buttonChanges=this._radios.changes.subscribe(()=>{this.selected&&!this._radios.find(e=>e===this.selected)&&(this._selected=null)})}ngOnDestroy(){this._buttonChanges?.unsubscribe()}_touch(){this.onTouched&&this.onTouched()}_updateRadioButtonNames(){this._radios&&this._radios.forEach(e=>{e.name=this.name,e._markForCheck()})}_updateSelectedRadioFromValue(){let e=this._selected!==null&&this._selected.value===this._value;this._radios&&!e&&(this._selected=null,this._radios.forEach(i=>{i.checked=this.value===i.value,i.checked&&(this._selected=i)}))}_emitChangeEvent(){this._isInitialized&&this.change.emit(new Yh(this._selected,this._value))}_markRadiosForCheck(){this._radios&&this._radios.forEach(e=>e._markForCheck())}writeValue(e){this.value=e,this._changeDetector.markForCheck()}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this.onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetector.markForCheck()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["mat-radio-group"]],contentQueries:function(i,r,o){if(i&1&&yt(o,Zh,5),i&2){let s;U(s=$())&&(r._radios=s)}},hostAttrs:["role","radiogroup",1,"mat-mdc-radio-group"],inputs:{color:"color",name:"name",labelPosition:"labelPosition",value:"value",selected:"selected",disabled:[2,"disabled","disabled",V],required:[2,"required","required",V],disabledInteractive:[2,"disabledInteractive","disabledInteractive",V]},outputs:{change:"change"},exportAs:["matRadioGroup"],features:[Re([NH,{provide:Xk,useExisting:t}])]})}return t})(),Zh=(()=>{class t{_elementRef=d(P);_changeDetector=d(ue);_focusMonitor=d(Mn);_radioDispatcher=d(Xb);_defaultOptions=d(PH,{optional:!0});_ngZone=d(j);_renderer=d(we);_uniqueId=d(Ie).getId("mat-radio-");_cleanupClick;id=this._uniqueId;name;ariaLabel;ariaLabelledby;ariaDescribedby;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked!==e&&(this._checked=e,e&&this.radioGroup&&this.radioGroup.value!==this.value?this.radioGroup.selected=this:!e&&this.radioGroup&&this.radioGroup.value===this.value&&(this.radioGroup.selected=null),e&&this._radioDispatcher.notify(this.id,this.name),this._changeDetector.markForCheck())}get value(){return this._value}set value(e){this._value!==e&&(this._value=e,this.radioGroup!==null&&(this.checked||(this.checked=this.radioGroup.value===e),this.checked&&(this.radioGroup.selected=this)))}get labelPosition(){return this._labelPosition||this.radioGroup&&this.radioGroup.labelPosition||"after"}set labelPosition(e){this._labelPosition=e}_labelPosition;get disabled(){return this._disabled||this.radioGroup!==null&&this.radioGroup.disabled}set disabled(e){this._setDisabled(e)}get required(){return this._required||this.radioGroup&&this.radioGroup.required}set required(e){e!==this._required&&this._changeDetector.markForCheck(),this._required=e}get color(){return this._color||this.radioGroup&&this.radioGroup.color||this._defaultOptions&&this._defaultOptions.color||"accent"}set color(e){this._color=e}_color;get disabledInteractive(){return this._disabledInteractive||this.radioGroup!==null&&this.radioGroup.disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e}_disabledInteractive;change=new H;radioGroup;get inputId(){return`${this.id||this._uniqueId}-input`}_checked=!1;_disabled=!1;_required=!1;_value=null;_removeUniqueSelectionListener=()=>{};_previousTabIndex;_inputElement;_rippleTrigger;_noopAnimations=ke();_injector=d(X);constructor(){d(qe).load(Tn);let e=d(Xk,{optional:!0}),i=d(new Jn("tabindex"),{optional:!0});this.radioGroup=e,this._disabledInteractive=this._defaultOptions?.disabledInteractive??!1,i&&(this.tabIndex=ut(i,0))}focus(e,i){i?this._focusMonitor.focusVia(this._inputElement,i,e):this._inputElement.nativeElement.focus(e)}_markForCheck(){this._changeDetector.markForCheck()}ngOnInit(){this.radioGroup&&(this.checked=this.radioGroup.value===this._value,this.checked&&(this.radioGroup.selected=this),this.name=this.radioGroup.name),this._removeUniqueSelectionListener=this._radioDispatcher.listen((e,i)=>{e!==this.id&&i===this.name&&(this.checked=!1)})}ngDoCheck(){this._updateTabIndex()}ngAfterViewInit(){this._updateTabIndex(),this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{!e&&this.radioGroup&&this.radioGroup._touch()}),this._ngZone.runOutsideAngular(()=>{this._cleanupClick=this._renderer.listen(this._inputElement.nativeElement,"click",this._onInputClick)})}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._removeUniqueSelectionListener()}_emitChangeEvent(){this.change.emit(new Yh(this,this._value))}_isRippleDisabled(){return this.disableRipple||this.disabled}_onInputInteraction(e){if(e.stopPropagation(),!this.checked&&!this.disabled){let i=this.radioGroup&&this.value!==this.radioGroup.value;this.checked=!0,this._emitChangeEvent(),this.radioGroup&&(this.radioGroup._controlValueAccessorChangeFn(this.value),i&&this.radioGroup._emitChangeEvent())}}_onTouchTargetClick(e){this._onInputInteraction(e),(!this.disabled||this.disabledInteractive)&&this._inputElement?.nativeElement.focus()}_setDisabled(e){this._disabled!==e&&(this._disabled=e,this._changeDetector.markForCheck())}_onInputClick=e=>{this.disabled&&this.disabledInteractive&&e.preventDefault()};_updateTabIndex(){let e=this.radioGroup,i;if(!e||!e.selected||this.disabled?i=this.tabIndex:i=e.selected===this?this.tabIndex:-1,i!==this._previousTabIndex){let r=this._inputElement?.nativeElement;r&&(r.setAttribute("tabindex",i+""),this._previousTabIndex=i,bt(()=>{queueMicrotask(()=>{e&&e.selected&&e.selected!==this&&document.activeElement===r&&(e.selected?._inputElement.nativeElement.focus(),document.activeElement===r&&this._inputElement.nativeElement.blur())})},{injector:this._injector}))}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-radio-button"]],viewQuery:function(i,r){if(i&1&&je(AH,5)(RH,7,P),i&2){let o;U(o=$())&&(r._inputElement=o.first),U(o=$())&&(r._rippleTrigger=o.first)}},hostAttrs:[1,"mat-mdc-radio-button"],hostVars:19,hostBindings:function(i,r){i&1&&R("focus",function(){return r._inputElement.nativeElement.focus()}),i&2&&(Y("id",r.id)("tabindex",null)("aria-label",null)("aria-labelledby",null)("aria-describedby",null),z("mat-primary",r.color==="primary")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("mat-mdc-radio-checked",r.checked)("mat-mdc-radio-disabled",r.disabled)("mat-mdc-radio-disabled-interactive",r.disabledInteractive)("_mat-animation-noopable",r._noopAnimations))},inputs:{id:"id",name:"name",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],disableRipple:[2,"disableRipple","disableRipple",V],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:ut(e)],checked:[2,"checked","checked",V],value:"value",labelPosition:"labelPosition",disabled:[2,"disabled","disabled",V],required:[2,"required","required",V],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",V]},outputs:{change:"change"},exportAs:["matRadioButton"],ngContentSelectors:OH,decls:13,vars:17,consts:[["formField",""],["input",""],["mat-internal-form-field","",3,"labelPosition"],[1,"mdc-radio"],["aria-hidden","true",1,"mat-mdc-radio-touch-target",3,"click"],["type","radio","aria-invalid","false",1,"mdc-radio__native-control",3,"change","id","checked","disabled","required"],["aria-hidden","true",1,"mdc-radio__background"],[1,"mdc-radio__outer-circle"],[1,"mdc-radio__inner-circle"],["mat-ripple","","aria-hidden","true",1,"mat-radio-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mat-ripple-element","mat-radio-persistent-ripple"],[1,"mdc-label",3,"for"]],template:function(i,r){i&1&&(Se(),f(0,"div",2,0)(2,"div",3)(3,"div",4),R("click",function(s){return r._onTouchTargetClick(s)}),h(),f(4,"input",5,1),R("change",function(s){return r._onInputInteraction(s)}),h(),f(6,"div",6),M(7,"div",7)(8,"div",8),h(),f(9,"div",9),M(10,"div",10),h()(),f(11,"label",11),se(12),h()()),i&2&&(D("labelPosition",r.labelPosition),p(2),z("mdc-radio--disabled",r.disabled),p(2),D("id",r.inputId)("checked",r.checked)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),Y("name",r.name)("value",r.value)("aria-label",r.ariaLabel)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),p(5),D("matRippleTrigger",r._rippleTrigger.nativeElement)("matRippleDisabled",r._isRippleDisabled())("matRippleCentered",!0),p(2),D("for",r.inputId))},dependencies:[tr,Wh],styles:[`.mat-mdc-radio-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-radio-button .mdc-radio {
  display: inline-block;
  position: relative;
  flex: 0 0 auto;
  box-sizing: content-box;
  width: 20px;
  height: 20px;
  cursor: pointer;
  will-change: opacity, transform, border-color, color;
  padding: calc((var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:not([disabled]):not(:focus) ~ .mdc-radio__background::before {
  opacity: 0.04;
  transform: scale(1);
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:not([disabled]) ~ .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-hover-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-hover-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-hover-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-pressed-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-pressed-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-pressed-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__background {
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  width: 20px;
  height: 20px;
}
.mat-mdc-radio-button .mdc-radio__background::before {
  position: absolute;
  transform: scale(0, 0);
  border-radius: 50%;
  opacity: 0;
  pointer-events: none;
  content: "";
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
  top: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
  left: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-radio-button .mdc-radio__outer-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  border-width: 2px;
  border-style: solid;
  border-radius: 50%;
  transition: border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-radio-button .mdc-radio__inner-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  transform: scale(0);
  border-radius: 50%;
  transition: transform 90ms cubic-bezier(0.4, 0, 0.6, 1), background-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
@media (forced-colors: active) {
  .mat-mdc-radio-button .mdc-radio__inner-circle {
    background-color: CanvasText !important;
  }
}
.mat-mdc-radio-button .mdc-radio__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  top: 0;
  right: 0;
  left: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background {
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:focus + .mdc-radio__background::before {
  transform: scale(1);
  opacity: 0.12;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-unselected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background {
  cursor: default;
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface, currentColor));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:focus:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-focus-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:focus:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-focus-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  transform: scale(0.5);
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled {
  pointer-events: auto;
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-unselected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled:hover .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:checked:focus + .mdc-radio__background > .mdc-radio__outer-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled:hover .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:checked:focus + .mdc-radio__background > .mdc-radio__inner-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface, currentColor));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__background::before,
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__outer-circle,
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__inner-circle {
  transition: none !important;
}
.mat-mdc-radio-button label {
  cursor: pointer;
}
.mat-mdc-radio-button label:empty {
  display: none;
}
.mat-mdc-radio-button .mdc-radio__background::before {
  background-color: var(--mat-radio-ripple-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button.mat-mdc-radio-checked .mat-ripple-element,
.mat-mdc-radio-button.mat-mdc-radio-checked .mdc-radio__background::before {
  background-color: var(--mat-radio-checked-ripple-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mat-ripple-element,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__background::before {
  background-color: var(--mat-radio-ripple-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mat-internal-form-field {
  color: var(--mat-radio-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-radio-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-radio-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-radio-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-radio-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-radio-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-radio-button .mdc-radio--disabled + label {
  color: var(--mat-radio-disabled-label-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-radio-button .mat-radio-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: 50%;
}
.mat-mdc-radio-button .mat-radio-ripple > .mat-ripple-element {
  opacity: 0.14;
}
.mat-mdc-radio-button .mat-radio-ripple::before {
  border-radius: 50%;
}
.mat-mdc-radio-button .mdc-radio > .mdc-radio__native-control:focus:enabled:not(:checked) ~ .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-focus-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button.cdk-focused .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-radio-disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-radio-disabled.mat-mdc-radio-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-radio-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-radio-touch-target-size, 48px);
  width: var(--mat-radio-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--mat-radio-touch-target-display, block);
}
[dir=rtl] .mat-mdc-radio-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2,changeDetection:0})}return t})(),Kk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[uo,Zh,ye]})}return t})();var LH=(t,n)=>n.id;function BH(t,n){if(t&1&&(f(0,"label",1)(1,"mat-radio-button",2)(2,"div",3)(3,"strong"),v(4),Ee(5,"currency"),h(),f(6,"span",4),v(7),h()()()()),t&2){let e=n.$implicit;p(),D("value",e.id),p(3),Rm("",e.shortName," - ",et(5,4,e.price,"GBP")),p(3),Z(e.description)}}var Qh=class t{checkoutS=d(qh);cartS=d(it);stripeService=d(rc);deliveryComplete=fD();ngOnInit(){this.checkoutS.getDeliveryMethods().subscribe({next:n=>{if(this.cartS.cart()?.deliveryMethodId){let e=n.find(i=>i.id===this.cartS.cart()?.deliveryMethodId);e&&(this.cartS.selectedDelivery.set(e),this.deliveryComplete.emit(!0))}}})}updateDeliveryMethod(n){let e=this.checkoutS.deliveryMethods.find(r=>r.id===n);if(!e)return;this.cartS.selectedDelivery.set(e);let i=this.cartS.cart();i&&(i.deliveryMethodId=e.id,this.cartS.setCart(i),this.deliveryComplete.emit(!0))}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-checkout-delivery"]],outputs:{deliveryComplete:"deliveryComplete"},decls:4,vars:1,consts:[[1,"grid","grid-cols-2","gap-4",3,"change","value"],[1,"p-3","border","border-gray-200","cursor-pointer","w-full","h-full","hover:bg-purple-100"],[1,"w-full","h-full",3,"value"],[1,"flex","flex-col","w-full","h-full"],[1,"text-sm"]],template:function(e,i){if(e&1&&(f(0,"div")(1,"mat-radio-group",0),R("change",function(o){return i.updateDeliveryMethod(o.value)}),st(2,BH,8,7,"label",1,LH),h()()),e&2){let r;p(),D("value",(r=i.cartS.selectedDelivery())==null?null:r.id),p(),at(i.checkoutS.deliveryMethods)}},dependencies:[Kk,By,Zh,kt],encapsulation:2})};var Eo=class t{transform(n,...e){if(n&&"address"in n&&n.name){let{line1:i,line2:r,city:o,state:s,country:a,postal_code:c}=n?.address;return`${n.name}, ${i}. ${r?", "+r:""}, ${o},
       ${s}, ${c}, ${a}`}else if(n&&"line1"in n){let{line1:i,line2:r,city:o,state:s,country:a,postalCode:c}=n;return`${n.name}, ${i}. ${r?", "+r:""}, ${o},
       ${s}, ${c}, ${a}`}else return"Unknown address"}static \u0275fac=function(e){return new(e||t)};static \u0275pipe=Kr({name:"address",type:t,pure:!0})};var So=class t{transform(n,...e){if(n&&"card"in n){let{brand:i,last4:r,exp_month:o,exp_year:s}=n.card;return`${i.toLocaleUpperCase()} **** **** **** ${r}, Exp: ${o}/${s}`}else if(n&&"last4"in n){let{brand:i,last4:r,expMonth:o,expYear:s}=n;return`${i.toLocaleUpperCase()} **** **** **** ${r}, Exp: ${o}/${s}`}else return"Unknow Payment Method"}static \u0275fac=function(e){return new(e||t)};static \u0275pipe=Kr({name:"paymentCard",type:t,pure:!0})};var jH=(t,n)=>n.productId;function VH(t,n){if(t&1&&(Ne(0,"tr")(1,"td",8)(2,"div",9),zt(3,"img",10),Ne(4,"span"),v(5),Ge()()(),Ne(6,"td",11),v(7),Ge(),Ne(8,"td",12),v(9),Ee(10,"currency"),Ge()()),t&2){let e=n.$implicit;p(3),ft("src",Xt(e.imageUrl),Zn),p(2),Z(e.productName),p(2),De("x",e.quantityItems),p(2),Z(et(10,5,e.price,"GBP"))}}var Xh=class t{cartS=d(it);confirmationToken;static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-checkout-review"]],inputs:{confirmationToken:"confirmationToken"},decls:20,vars:6,consts:[[1,"my-4","w-full"],[1,"text-lg","font-semibold"],[1,"font-medium"],[1,"mt-1","text-gray-500"],[1,"mt-6","mx-auto"],[1,"border-b","border-gray-200"],[1,"w-full","text-center"],[1,"divide-y","divide-gray-200"],[1,"py-4"],[1,"flex","items-center","gap-4"],["alt","product image",1,"w-10","h-10",3,"src"],[1,"p-4"],[1,"p-4","text-right"]],template:function(e,i){if(e&1&&(Ne(0,"div",0)(1,"h4",1),v(2,"Billing and delivery information"),Ge(),Ne(3,"dl")(4,"dt",2),v(5,"Shipping address"),Ge(),Ne(6,"dd",3),v(7),Ee(8,"address"),Ge(),Ne(9,"dt",2),v(10,"Payment details"),Ge(),Ne(11,"dd",3),v(12),Ee(13,"paymentCard"),Ge()(),Ne(14,"div",4)(15,"div",5)(16,"table",6)(17,"tbody",7),st(18,VH,11,8,"tr",null,jH),Ge()()()()()),e&2){let r;p(7),Z(Sn(8,2,i.confirmationToken==null?null:i.confirmationToken.shipping)),p(5),Z(Sn(13,4,i.confirmationToken==null?null:i.confirmationToken.payment_method_preview)),p(6),at((r=i.cartS.cart())==null?null:r.items)}},dependencies:[kt,Eo,So],encapsulation:2})};var HH=["determinateSpinner"];function zH(t,n){if(t&1&&(Wt(),f(0,"svg",11),M(1,"circle",12),h()),t&2){let e=S();Y("viewBox",e._viewBox()),p(),Qn("stroke-dasharray",e._strokeCircumference(),"px")("stroke-dashoffset",e._strokeCircumference()/2,"px")("stroke-width",e._circleStrokeWidth(),"%"),Y("r",e._circleRadius())}}var UH=new y("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:Jk})}),Jk=100,$H=10,Kh=(()=>{class t{_elementRef=d(P);_noopAnimations;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";_determinateCircle;constructor(){let e=d(UH),i=ql(),r=this._elementRef.nativeElement;this._noopAnimations=i==="di-disabled"&&!!e&&!e._forceAnimations,this.mode=r.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&i==="reduced-motion"&&r.classList.add("mat-progress-spinner-reduced-motion"),e&&(e.color&&(this.color=this._defaultColor=e.color),e.diameter&&(this.diameter=e.diameter),e.strokeWidth&&(this.strokeWidth=e.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(e){this._value=Math.max(0,Math.min(100,e||0))}_value=0;get diameter(){return this._diameter}set diameter(e){this._diameter=e||0}_diameter=Jk;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(e){this._strokeWidth=e||0}_strokeWidth;_circleRadius(){return(this.diameter-$H)/2}_viewBox(){let e=this._circleRadius()*2+this.strokeWidth;return`0 0 ${e} ${e}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(i,r){if(i&1&&je(HH,5),i&2){let o;U(o=$())&&(r._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(i,r){i&2&&(Y("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",r.mode==="determinate"?r.value:null)("mode",r.mode),It("mat-"+r.color),Qn("width",r.diameter,"px")("height",r.diameter,"px")("--mat-progress-spinner-size",r.diameter+"px")("--mat-progress-spinner-active-indicator-width",r.diameter+"px"),z("_mat-animation-noopable",r._noopAnimations)("mdc-circular-progress--indeterminate",r.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",ut],diameter:[2,"diameter","diameter",ut],strokeWidth:[2,"strokeWidth","strokeWidth",ut]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(i,r){if(i&1&&(Be(0,zH,2,8,"ng-template",null,0,Kn),f(2,"div",2,1),Wt(),f(4,"svg",3),M(5,"circle",4),h()(),$i(),f(6,"div",5)(7,"div",6)(8,"div",7),cn(9,8),h(),f(10,"div",9),cn(11,8),h(),f(12,"div",10),cn(13,8),h()()()),i&2){let o=Ve(1);p(4),Y("viewBox",r._viewBox()),p(),Qn("stroke-dasharray",r._strokeCircumference(),"px")("stroke-dashoffset",r._strokeDashOffset(),"px")("stroke-width",r._circleStrokeWidth(),"%"),Y("r",r._circleRadius()),p(4),D("ngTemplateOutlet",o),p(2),D("ngTemplateOutlet",o),p(2),D("ngTemplateOutlet",o)}},dependencies:[Xi],styles:[`.mat-mdc-progress-spinner {
  --mat-progress-spinner-animation-multiplier: 1;
  display: block;
  overflow: hidden;
  line-height: 0;
  position: relative;
  direction: ltr;
  transition: opacity 250ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-progress-spinner circle {
  stroke-width: var(--mat-progress-spinner-active-indicator-width, 4px);
}
.mat-mdc-progress-spinner._mat-animation-noopable, .mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__determinate-circle {
  transition: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-circle-graphic,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__spinner-layer,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container {
  animation: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container circle {
  stroke-dasharray: 0 !important;
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic,
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle {
    stroke: currentColor;
    stroke: CanvasText;
  }
}

.mat-progress-spinner-reduced-motion {
  --mat-progress-spinner-animation-multiplier: 1.25;
}

.mdc-circular-progress__determinate-container,
.mdc-circular-progress__indeterminate-circle-graphic,
.mdc-circular-progress__indeterminate-container,
.mdc-circular-progress__spinner-layer {
  position: absolute;
  width: 100%;
  height: 100%;
}

.mdc-circular-progress__determinate-container {
  transform: rotate(-90deg);
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__determinate-container {
  opacity: 0;
}

.mdc-circular-progress__indeterminate-container {
  font-size: 0;
  letter-spacing: 0;
  white-space: nowrap;
  opacity: 0;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__indeterminate-container {
  opacity: 1;
  animation: mdc-circular-progress-container-rotate calc(1568.2352941176ms * var(--mat-progress-spinner-animation-multiplier)) linear infinite;
}

.mdc-circular-progress__determinate-circle-graphic,
.mdc-circular-progress__indeterminate-circle-graphic {
  fill: transparent;
}

.mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
.mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
  stroke: var(--mat-progress-spinner-active-indicator-color, var(--mat-sys-primary));
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
    stroke: CanvasText;
  }
}

.mdc-circular-progress__determinate-circle {
  transition: stroke-dashoffset 500ms cubic-bezier(0, 0, 0.2, 1);
}

.mdc-circular-progress__gap-patch {
  position: absolute;
  top: 0;
  left: 47.5%;
  box-sizing: border-box;
  width: 5%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress__gap-patch .mdc-circular-progress__indeterminate-circle-graphic {
  left: -900%;
  width: 2000%;
  transform: rotate(180deg);
}
.mdc-circular-progress__circle-clipper .mdc-circular-progress__indeterminate-circle-graphic {
  width: 200%;
}
.mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  left: -100%;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-left .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-left-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-right-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

.mdc-circular-progress__circle-clipper {
  display: inline-flex;
  position: relative;
  width: 50%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress--indeterminate .mdc-circular-progress__spinner-layer {
  animation: mdc-circular-progress-spinner-layer-rotate calc(5332ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

@keyframes mdc-circular-progress-container-rotate {
  to {
    transform: rotate(360deg);
  }
}
@keyframes mdc-circular-progress-spinner-layer-rotate {
  12.5% {
    transform: rotate(135deg);
  }
  25% {
    transform: rotate(270deg);
  }
  37.5% {
    transform: rotate(405deg);
  }
  50% {
    transform: rotate(540deg);
  }
  62.5% {
    transform: rotate(675deg);
  }
  75% {
    transform: rotate(810deg);
  }
  87.5% {
    transform: rotate(945deg);
  }
  100% {
    transform: rotate(1080deg);
  }
}
@keyframes mdc-circular-progress-left-spin {
  from {
    transform: rotate(265deg);
  }
  50% {
    transform: rotate(130deg);
  }
  to {
    transform: rotate(265deg);
  }
}
@keyframes mdc-circular-progress-right-spin {
  from {
    transform: rotate(-265deg);
  }
  50% {
    transform: rotate(-130deg);
  }
  to {
    transform: rotate(-265deg);
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var eM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[ye]})}return t})();var oi=class t{baseUrl=Kt.apiUrl;http=d(dn);orderComplete=!1;createOrder(n){let e=localStorage.getItem("token");return this.http.post(this.baseUrl+"orders",n,{headers:{Authorization:`Bearer ${e}`}})}getOrdersForUser(){let n=localStorage.getItem("token");return this.http.get(this.baseUrl+"orders",{headers:{Authorization:`Bearer ${n}`}})}getOrderDetailed(n){let e=localStorage.getItem("token");return this.http.get(this.baseUrl+"orders/"+n,{headers:{Authorization:`Bearer ${e}`}})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};function WH(t,n){t&1&&M(0,"mat-spinner",20)}function qH(t,n){if(t&1&&(f(0,"span"),v(1),Ee(2,"currency"),h()),t&2){let e,i=S();p(),De("Pay ",et(2,1,(e=i.cartS.totals())==null?null:e.total,"GBP"))}}var Jh=class t{stripeService=d(rc);snackbar=d(Do);accountS=d(Zt);router=d(Mt);ngZone=d(j);cdr=d(ue);orderService=d(oi);signalrService=d(ar);cartS=d(it);addressElememt;paymentElement;saveAddress=!1;completionStatus=N({address:!1,card:!1,delivery:!1});confirmationToken;loading=!1;async ngOnInit(){try{this.addressElememt=await this.stripeService.createAddressElement(),this.addressElememt.mount("#address-element"),this.addressElememt.on("change",this.handleAddressChange),this.paymentElement=await this.stripeService.createPaymentElement(),this.paymentElement.mount("#payment-element"),this.paymentElement.on("change",n=>{this.ngZone.run(()=>{this.handlePaymentChange(n)})})}catch(n){this.snackbar.error(n.message)}}handleAddressChange=n=>{this.completionStatus.update(e=>J(b({},e),{address:n.complete}))};handlePaymentChange=n=>{this.completionStatus.update(e=>J(b({},e),{card:n.complete}))};handleDeliveryChange(n){this.completionStatus.update(e=>(e.delivery=n,e))}async getConfirmationToken(){try{if(Object.values(this.completionStatus()).every(n=>n===!0)){let n=await this.stripeService.createConfirmationToken();if(n.error)throw new Error(n.error.message);this.ngZone.run(()=>{this.confirmationToken=n.confirmationToken,this.cdr.detectChanges()}),console.log(this.confirmationToken)}}catch(n){this.snackbar.error(n.message)}}async onStepChange(n){if(n.selectedIndex===1&&this.saveAddress){let e=await this.getAddressFromStripeAddress();e&&Bo(this.accountS.updateAddress(e))}n.selectedIndex===2&&await Bo(this.stripeService.createOrUpdatePaymentIntent()),n.selectedIndex===3&&(await this.getConfirmationToken(),this.ngZone.run(()=>{}))}async confirmPayment(n){this.loading=!0;try{if(!this.confirmationToken)throw new Error("Confirmation token missing");let e=await this.stripeService.confirmPayment(this.confirmationToken);if(e.paymentIntent?.status==="succeeded"){let i=await this.createOrderModel(),r=await Bo(this.orderService.createOrder(i));if(r)this.orderService.orderComplete=!0,this.cartS.deletecart(),this.cartS.selectedDelivery.set(null),this.signalrService.orderSignal.set(r),this.router.navigateByUrl("/checkout/success");else throw new Error("Order Creation Failed")}else throw e.error?new Error(e.error.message):new Error("something went wrong");this.ngZone.run(()=>{})}catch(e){this.ngZone.run(()=>{this.snackbar.error(e.message||"Something went wrong"),n.previous()})}finally{this.ngZone.run(()=>{this.loading=!1})}}async createOrderModel(){let n=this.cartS.cart(),e=await this.getAddressFromStripeAddress();e.name=this.confirmationToken?.shipping?.name??"";let i=this.confirmationToken?.payment_method_preview.card;if(!n?.id||!n.deliveryMethodId||!i||!e)throw new Error("Problem creating order");return{cartId:n.buyerId,paymentSummary:{last4:+i.last4,brand:i?.brand,expMonth:i?.exp_month,expYear:i?.exp_year},deliveryMethodId:n?.deliveryMethodId,shippingAddress:e}}async getAddressFromStripeAddress(){let n=await this.addressElememt?.getValue(),e=n?.value.address;return e?{name:n.value.name??this.confirmationToken?.shipping?.name??"",line1:e.line1,line2:e.line2??"",city:e.city,country:e.country,state:e.state,postalCode:e.postal_code}:null}onSaveAddressCheckboxChange(n){this.saveAddress=n.checked}ngOnDestroy(){this.stripeService.disposeElements()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-checkoutcompo"]],decls:38,vars:11,consts:[["stepper",""],[1,"flex","mt-32","gap-6"],[1,"w-3/4"],[1,"bg-white","border","border-gray-200","shadow-sm",3,"selectionChange","linear"],["label","Address",3,"completed"],["id","address-element"],[1,"flex","justify-end","mt-1"],[3,"change","checked"],[1,"flex","justify-between","mt-6"],["routerLink","/shop","mat-stroked-button","",1,"z-0"],["matStepperNext","","mat-flat-button","",1,"z-0",3,"disabled"],["label","Shipping",3,"completed"],[3,"deliveryComplete"],["matStepperPrevious","","mat-stroked-button",""],["matStepperNext","","mat-flat-button","",3,"disabled"],["label","Payment",3,"completed"],["id","payment-element"],["label","Confirmation"],[3,"confirmationToken"],["mat-flat-button","",3,"click","disabled"],["diameter","20"],[1,"w-1/4"]],template:function(e,i){if(e&1){let r=dt();f(0,"div",1)(1,"div",2)(2,"mat-stepper",3,0),R("selectionChange",function(s){return i.onStepChange(s)}),f(4,"mat-step",4),M(5,"div",5),f(6,"div",6)(7,"mat-checkbox",7),R("change",function(s){return i.onSaveAddressCheckboxChange(s)}),v(8," Save As Default Address "),h()(),f(9,"div",8)(10,"button",9),v(11,"Continue Shopping"),h(),f(12,"button",10),v(13,"Next"),h()()(),f(14,"mat-step",11)(15,"app-checkout-delivery",12),R("deliveryComplete",function(s){return i.handleDeliveryChange(s)}),h(),f(16,"div",8)(17,"button",13),v(18,"Back"),h(),f(19,"button",14),v(20,"Next"),h()()(),f(21,"mat-step",15),M(22,"div",16),f(23,"div",8)(24,"button",13),v(25,"Back"),h(),f(26,"button",14),v(27,"Next"),h()()(),f(28,"mat-step",17),M(29,"app-checkout-review",18),f(30,"div",8)(31,"button",13),v(32,"Back"),h(),f(33,"button",19),R("click",function(){Me(r);let s=Ve(3);return Te(i.confirmPayment(s))}),F(34,WH,1,0,"mat-spinner",20)(35,qH,3,4,"span"),h()()()()(),f(36,"div",21),M(37,"app-order-summary"),h()()}e&2&&(p(2),D("linear",!0),p(2),D("completed",i.completionStatus().address),p(3),D("checked",i.saveAddress),p(5),D("disabled",!i.completionStatus().address),p(2),D("completed",i.completionStatus().delivery),p(5),D("disabled",!i.completionStatus().delivery),p(2),D("completed",i.completionStatus().card),p(5),D("disabled",!i.completionStatus().card),p(3),D("confirmationToken",i.confirmationToken),p(4),D("disabled",!i.confirmationToken||i.loading),p(),L(i.loading?34:35))},dependencies:[ec,Pk,My,Ty,Ok,Nk,nt,Hf,Qk,Ly,Qh,Xh,eM,Kh,kt],encapsulation:2})};var ep=class t{fb=d(fh);accountS=d(Zt);router=d(Mt);activatedRoute=d(yn);returnUrl="/shop";constructor(){let n=this.activatedRoute.snapshot.queryParams.returnUrl;n&&(this.returnUrl=n)}loginForm=this.fb.group({email:[""],password:[""]});onSubmit(){this.accountS.login(this.loginForm.value).subscribe({next:()=>{this.accountS.getUserInfo().subscribe,this.router.navigateByUrl(this.returnUrl)}})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-login"]],decls:18,vars:1,consts:[[1,"max-w-lg","mx-auto","mt-32","p-8","bg-white"],[3,"ngSubmit","formGroup"],[1,"text-center","mb-6"],[1,"text-3xl","font-semibold","text-primary"],["appearance","outline",1,"w-full","mb-4"],["formControlName","email","type","email","placeholder","name@example.com","matInput",""],["appearance","outline",1,"w-full"],["formControlName","password","type","password","placeholder","Password","matInput",""],[1,"newbutton","px-2","relative","-top-2"],["routerLink","/account/register","routerLinkActive","active"],["mat-flat-button","","type","submit",1,"w-full","mt-0.5"]],template:function(e,i){e&1&&(f(0,"mat-card",0)(1,"form",1),R("ngSubmit",function(){return i.onSubmit()}),f(2,"div",2)(3,"h1",3),v(4,"Login"),h()(),f(5,"mat-form-field",4)(6,"mat-label"),v(7,"Email Address"),h(),M(8,"input",5),h(),f(9,"mat-form-field",6)(10,"mat-label"),v(11,"Password"),h(),M(12,"input",7),h(),f(13,"button",8)(14,"a",9),v(15,"Create an Account"),h()(),f(16,"button",10),v(17," Sign in "),h()()()),e&2&&(p(),D("formGroup",i.loginForm))},dependencies:[Za,Ya,ir,ri,Wa,Ar,ud,co,Rn,Co,Hn,pt,nt],styles:[".newbutton[_ngcontent-%COMP%]:hover{text-decoration:underline}.newbutton[_ngcontent-%COMP%]{color:#00f}"]})};function YH(t,n){if(t&1&&(f(0,"mat-error"),v(1),h()),t&2){let e=S();p(),De(" ",e.label," is Required ")}}function ZH(t,n){t&1&&(f(0,"mat-error"),v(1," Email is invalid "),h())}var tp=class t{constructor(n){this.controlDir=n;this.controlDir.valueAccessor=this}label="";type="text";writeValue(n){}registerOnChange(n){}registerOnTouched(n){}get control(){return this.controlDir.control}static \u0275fac=function(e){return new(e||t)(ie(xn,2))};static \u0275cmp=E({type:t,selectors:[["app-text-input"]],inputs:{label:"label",type:"type"},decls:6,vars:8,consts:[["appearance","outline",1,"w-full","mb-4"],["matInput","",3,"formControl","type","placeholder"]],template:function(e,i){e&1&&(f(0,"mat-form-field",0)(1,"mat-label"),v(2),h(),M(3,"input",1),F(4,YH,2,1,"mat-error"),F(5,ZH,2,0,"mat-error"),h()),e&2&&(p(2),Z(i.label),p(),D("type",Xt(i.type))("placeholder",Xt(i.label))("formControl",i.control),p(),L(i.control.hasError("required")?4:-1),p(),L(i.control.hasError("email")?5:-1))},dependencies:[Za,ir,ri,my,Rn,Co,gd,Hn],encapsulation:2})};function QH(t,n){if(t&1&&(f(0,"li"),v(1),h()),t&2){let e=n.$implicit;p(),Z(e)}}function XH(t,n){if(t&1&&(f(0,"div",8)(1,"ul",10),st(2,QH,2,1,"li",null,eo),h()()),t&2){let e=S();p(2),at(e.validationErrors())}}var np=class t{fb=d(fh);accountS=d(Zt);router=d(Mt);snack=d(Do);validationErrors=N([]);registerForm=this.fb.group({firstName:["",Si.required],lastName:["",Si.required],email:["",Si.required],password:["",Si.required]});onSubmit(){this.accountS.register(this.registerForm.value).subscribe({next:()=>{this.snack.success("Registration successful - you can now login"),this.router.navigateByUrl("/account/login")},error:n=>{let e=n.error?.errors;this.validationErrors.set(e?Object.values(e).flat():["Something went wrong"])}})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-register"]],decls:12,vars:3,consts:[[1,"max-w-lg","mx-auto","mt-32","p-8","bg-white"],[3,"ngSubmit","formGroup"],[1,"text-center","mb-6"],[1,"text-3xl","font-semibold","text-primary"],["label","First Name","formControlName","firstName","type","text"],["label","Last Name","formControlName","lastName","type","text"],["label","Email Address","formControlName","email","type","email"],["label","Password","formControlName","password","type","password"],[1,"mb-3","p-4","bg-red-100","text-red-600"],["mat-flat-button","","type","submit",1,"w-full","py-2",3,"disabled"],[1,"list-disc","px-3"]],template:function(e,i){e&1&&(f(0,"mat-card",0)(1,"form",1),R("ngSubmit",function(){return i.onSubmit()}),f(2,"div",2)(3,"h1",3),v(4,"Register"),h()(),M(5,"app-text-input",4)(6,"app-text-input",5)(7,"app-text-input",6)(8,"app-text-input",7),F(9,XH,4,0,"div",8),f(10,"button",9),v(11," Register "),h()()()),e&2&&(p(),D("formGroup",i.registerForm),p(8),L(i.validationErrors().length?9:-1),p(),D("disabled",i.registerForm.invalid))},dependencies:[Za,Ya,ri,Wa,Ar,ud,co,pt,tp],encapsulation:2})};var kd=(t,n)=>{let e=d(Zt),i=d(Mt);return e.currentUser()?!0:(i.navigate(["/account/login"],{queryParams:{returnUrl:n.url}}),!1)};var tM=(t,n)=>{let e=d(it),i=d(Mt),r=d(Do);return!e.cart()||e.cart()?.items.length===0?(r.error("Your cart is empty"),i.navigateByUrl("/cart"),!1):!0};function KH(t,n){if(t&1&&(f(0,"section",0)(1,"div",1)(2,"h2",2),v(3,"Thanks for your order!"),h(),f(4,"p",3),v(5," Your Order "),f(6,"span",4),v(7),h(),v(8," will not be processed becasue this shop is fake. "),h(),f(9,"div",5)(10,"dl",6)(11,"dt",7),v(12,"Date"),h(),f(13,"dd",8),v(14),Ee(15,"date"),h()(),f(16,"dl",6)(17,"dt",7),v(18,"Payment method"),h(),f(19,"dd",8),v(20),Ee(21,"paymentCard"),h()(),f(22,"dl",6)(23,"dt",7),v(24,"Address"),h(),f(25,"dd",8),v(26),Ee(27,"address"),h()(),f(28,"dl",6)(29,"dt",7),v(30,"Amount"),h(),f(31,"dd",8),v(32),Ee(33,"currency"),h()(),f(34,"div",9)(35,"button",10),v(36,"View Your Order"),h(),f(37,"button",11),v(38,"Continue Shopping"),h()()()()()),t&2){let e=n;p(7),De("#",e.id),p(7),Z(et(15,7,e.orderDate,"medium")),p(6),Z(Sn(21,10,e.paymentSummary)),p(6),Z(Sn(27,12,e.shippingAddress)),p(6),Z(et(33,14,e.total,"GBP")),p(3),D("routerLink",Xn("/orders/",e.id))}}function JH(t,n){t&1&&(f(0,"section",0)(1,"div",1)(2,"h2",2),v(3,"Order processing, Please wait"),h(),f(4,"div",5)(5,"div",12),M(6,"mat-spinner",13),f(7,"p",14),v(8,"Loading Order..."),h(),f(9,"span"),v(10,"Your Payment has been received, we are creating the order"),h()()(),f(11,"div",9)(12,"button",11),v(13,"Continue Shopping"),h()()()())}var ip=class t{signalrService=d(ar);orderServ=d(oi);ngOnDestroy(){this.orderServ.orderComplete=!1,this.signalrService.orderSignal.set(null)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-checkout-success"]],decls:2,vars:1,consts:[[1,"bg-white","py-16"],[1,"mx-auto","max-w-2xl","px-4"],[1,"font-semibold","text-2xl","mb-2"],[1,"text-gray-500","mb-8"],[1,"font-medium"],[1,"space-y-2","rounded-lg","border","border-gray-100","bg-gray-50","p-6","mb-8"],[1,"flex","items-center","justify-between","gap-4"],[1,"font-normal","text-gray-500"],[1,"font-medium","text-gray-900","text-end"],[1,"flex","items-center","space-x-4"],["mat-flat-button","",3,"routerLink"],["routerLink","/shop","mat-flat-button",""],[1,"flex","flex-col","justify-center","items-center"],["diameter","30"],[1,"text-xl"]],template:function(e,i){if(e&1&&F(0,KH,39,17,"section",0)(1,JH,14,0,"section",0),e&2){let r;L((r=i.signalrService.orderSignal())?0:1,r)}},dependencies:[pt,nt,Kh,ls,Eo,kt,So],encapsulation:2})};var ez=(t,n)=>n.id;function tz(t,n){t&1&&M(0,"div",1)}function nz(t,n){if(t&1&&(f(0,"tr",10)(1,"th",8),v(2),h(),f(3,"td",11),v(4),Ee(5,"date"),h(),f(6,"td",11),v(7),Ee(8,"currency"),h(),f(9,"td",11),v(10),h()()),t&2){let e=n.$implicit;D("routerLink",Xn("/orders/",e.id)),p(2),De("# ",e.id),p(2),Z(et(5,6,e.orderDate,"medium")),p(3),Z(et(8,9,e.total,"GBP")),p(3),Z(e.status)}}function iz(t,n){if(t&1&&(f(0,"h2",2),v(1,"My Orders"),h(),f(2,"div",3)(3,"div",4)(4,"table",5)(5,"thead",6)(6,"tr",7)(7,"th",8),v(8,"Order"),h(),f(9,"th",8),v(10,"Date"),h(),f(11,"th",8),v(12,"Total"),h(),f(13,"th",8),v(14,"Status"),h()()(),f(15,"tbody",9),st(16,nz,11,12,"tr",10,ez),h()()()()),t&2){let e=S();p(16),at(e.orders)}}var rp=class t{orderService=d(oi);cdr=d(ue);loading=!0;orders=[];ngOnInit(){this.orderService.getOrdersForUser().subscribe({next:n=>{this.orders=n,this.loading=!1,this.cdr.detectChanges()},error:n=>{console.log(n),this.loading=!1}})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-order-compo"]],decls:3,vars:1,consts:[[1,"mx-auto","mt-32"],[1,"flex","justify-center","items-center","min-h-75"],[1,"font-semibold","text-2xl","mb-6","text-center"],[1,"flex","flex-col"],[1,"w-full"],[1,"min-w-full","divide-y","divide-gray-200","cursor-pointer"],[1,"bg-gray-50"],[1,"uppercase","text-gray-600","text-sm"],[1,"text-center","px-6","py-3"],[1,"bg-white","divide-y","divide-gray-200"],[1,"hover:bg-gray-100",3,"routerLink"],[1,"text-center","px-6","py-3","font-medium"]],template:function(e,i){e&1&&(f(0,"div",0),F(1,tz,1,0,"div",1)(2,iz,18,0),h()),e&2&&(p(),L(i.loading?1:2))},dependencies:[nt,ls,kt],encapsulation:2})};var rz=(t,n)=>n.id;function oz(t,n){if(t&1&&(f(0,"tr")(1,"td",22)(2,"div",23),M(3,"img",24),f(4,"span"),v(5),h()()(),f(6,"td",25),v(7),h(),f(8,"td",26),v(9),Ee(10,"currency"),h()()),t&2){let e=n.$implicit;p(3),D("src",Xt(e.productsOrdered==null?null:e.productsOrdered.imageUrl),Zn),p(2),Z(e.productsOrdered==null?null:e.productsOrdered.productName),p(2),De("x",e.quantity),p(2),Z(et(10,5,e.price,"GBP"))}}function sz(t,n){if(t&1&&(f(0,"mat-card",0)(1,"div",1)(2,"div",2)(3,"h2",3),v(4),h(),f(5,"button",4),v(6,"Return To Orders"),h()(),f(7,"div",5)(8,"div",6)(9,"h4",7),v(10," Billing and Delivery Information"),h(),f(11,"dl")(12,"dt",8),v(13,"Shipping Address"),h(),f(14,"dd",9),v(15),Ee(16,"address"),h()(),f(17,"dl")(18,"dt",8),v(19,"Payment Info"),h(),f(20,"dd",9),v(21),Ee(22,"paymentCard"),h()()(),f(23,"div",6)(24,"h4",7),v(25," Order Details"),h(),f(26,"dl")(27,"dt",8),v(28,"Email Address"),h(),f(29,"dd",9),v(30),h()(),f(31,"dl")(32,"dt",8),v(33,"Order Status"),h(),f(34,"dd",9),v(35),h(),f(36,"dd",9),v(37),Ee(38,"date"),h()()()(),f(39,"div",10)(40,"div",11)(41,"table",12)(42,"tbody",13),st(43,oz,11,8,"tr",null,rz),h()()()(),f(45,"div",14)(46,"p",15),v(47,"Order Summary"),h(),f(48,"div",16)(49,"div",6)(50,"dl",17)(51,"dt",18),v(52,"Subtotal"),h(),f(53,"dd",19),v(54),Ee(55,"currency"),h()(),f(56,"dl",17)(57,"dt",18),v(58,"Discount"),h(),f(59,"dd",20),v(60,"-0.00"),h()(),f(61,"dl",17)(62,"dt",18),v(63,"Delivery Fee"),h(),f(64,"dd",19),v(65),Ee(66,"currency"),h()()(),f(67,"dl",21)(68,"dt",18),v(69,"Total"),h(),f(70,"dd",19),v(71),Ee(72,"currency"),h()()()()()()),t&2){let e=S();p(4),De("Order Summary For Order#",e.order.id),p(11),Z(Sn(16,9,e.order.shippingAddress)),p(6),Z(Sn(22,11,e.order.paymentSummary)),p(9),Z(e.order.buyerEmail),p(5),Z(e.order.status),p(2),Z(et(38,13,e.order.orderDate,"medium")),p(6),at(e.order.orderItems),p(11),Z(et(55,16,e.order.subtotal,"GBP")),p(11),Z(et(66,19,e.order.deliveryMethod.price,"GBP")),p(6),Z(et(72,22,e.order.total,"GBP"))}}var op=class t{orderService=d(oi);activatedRoute=d(yn);order;ngOnInit(){this.loadOrder()}loadOrder(){let n=this.activatedRoute.snapshot.paramMap.get("id");n&&this.orderService.getOrderDetailed(+n).subscribe({next:e=>this.order=e})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-order-detailed"]],decls:1,vars:1,consts:[[1,"bg-white","py-8","shadow-md","max-w-5xl","mx-auto"],[1,"px-4","w-full"],[1,"flex","justify-between","items-center","align-middle"],[1,"text-2xl","text-center","font-semibold"],["routerLink","/orders","mat-stroked-button",""],[1,"mt-8","py-3","border-t","border-gray-200","flex","gap-16"],[1,"space-y-2"],[1,"text-lg","font-semibold"],[1,"font-medium"],[1,"mt-1","font-light"],[1,"mt-4"],[1,"border-y","border-gray-200"],[1,"w-full","text-center"],[1,"divide-y","divide-gray-200"],[1,"space-y-4","rounded-lg","border-y","border-gray-200","p-4","bg-white"],[1,"text-xl","font-semibold"],[1,"space-y-4"],[1,"flex","items-center","justify-between","gap-4"],[1,"font-medium","text-gray-500"],[1,"font-medium","text-gray-900"],[1,"font-medium","text-green-600"],[1,"flex","items-center","justify-between","gap-4","border-t","border-gray-200","pt-2"],[1,"py-4"],[1,"flex","items-center","gap-4"],["alt","product image",1,"w-10","h-10",3,"src"],[1,"p-4"],[1,"p-4","text-right"]],template:function(e,i){e&1&&F(0,sz,73,25,"mat-card",0),e&2&&L(i.order?0:-1)},dependencies:[uS,co,pt,nt,ls,kt,Eo,So],encapsulation:2})};var nM=(t,n)=>{let e=d(oi),i=d(Mt);return e.orderComplete?!0:(i.navigateByUrl("/shop"),!1)};var iM=[{path:"",component:Mf},{path:"shop",component:vh},{path:"shop/:id",component:bh},{path:"cart",component:xh},{path:"checkout",component:Jh,canActivate:[kd,tM]},{path:"checkout/success",component:ip,canActivate:[kd,nM]},{path:"orders",component:rp,canActivate:[kd]},{path:"orders/:id",component:op,canActivate:[kd]},{path:"account/login",component:ep},{path:"account/register",component:np},{path:"**",redirectTo:"",pathMatch:"full"}];var sc=class t{cartService=d(it);accountS=d(Zt);signalrService=d(ar);init(){let n=localStorage.getItem("token");return console.log("TOKEN ON REFRESH:",n),jo({cart:this.cartService.getCart().pipe(di(()=>Q(null))),user:n?this.accountS.getUserInfo():Q(null).pipe(ct(e=>{e&&this.signalrService.createHubConnection()}))})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var ac=class t{loading=N(!1);busyRequestCount=0;busy(){setTimeout(()=>{this.busyRequestCount++,this.loading.set(!0)},0)}idle(){setTimeout(()=>{this.busyRequestCount--,this.busyRequestCount<=0&&(this.busyRequestCount=0,this.loading.set(!1))},0)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var rM=(t,n)=>{let e=d(ac);return e.busy(),n(t).pipe(Kt.production?nn:Ip(500),Ni(()=>e.idle()))};var oM=(t,n)=>{let e=t.clone({withCredentials:!0});return n(e)};function az(t){return()=>Ep(t.init()).finally(()=>{let n=document.getElementById("initial-splash");n&&n.remove()})}var sM={providers:[Og(),wb(iM),qv(Yv([rM,oM])),Sm(()=>az(d(sc))())]};var aM="mat-badge-content",cz=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-badge {
  position: relative;
}
.mat-badge.mat-badge {
  overflow: visible;
}

.mat-badge-content {
  position: absolute;
  text-align: center;
  display: inline-block;
  transition: transform 200ms ease-in-out;
  transform: scale(0.6);
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
  box-sizing: border-box;
  pointer-events: none;
  background-color: var(--mat-badge-background-color, var(--mat-sys-error));
  color: var(--mat-badge-text-color, var(--mat-sys-on-error));
  font-family: var(--mat-badge-text-font, var(--mat-sys-label-small-font));
  font-weight: var(--mat-badge-text-weight, var(--mat-sys-label-small-weight));
  border-radius: var(--mat-badge-container-shape, var(--mat-sys-corner-full));
}
.mat-badge-above .mat-badge-content {
  bottom: 100%;
}
.mat-badge-below .mat-badge-content {
  top: 100%;
}
.mat-badge-before .mat-badge-content {
  right: 100%;
}
[dir=rtl] .mat-badge-before .mat-badge-content {
  right: auto;
  left: 100%;
}
.mat-badge-after .mat-badge-content {
  left: 100%;
}
[dir=rtl] .mat-badge-after .mat-badge-content {
  left: auto;
  right: 100%;
}
@media (forced-colors: active) {
  .mat-badge-content {
    outline: solid 1px;
    border-radius: 0;
  }
}

.mat-badge-disabled .mat-badge-content {
  background-color: var(--mat-badge-disabled-state-background-color, color-mix(in srgb, var(--mat-sys-error) 38%, transparent));
  color: var(--mat-badge-disabled-state-text-color, var(--mat-sys-on-error));
}

.mat-badge-hidden .mat-badge-content {
  display: none;
}

.ng-animate-disabled .mat-badge-content,
.mat-badge-content._mat-animation-noopable {
  transition: none;
}

.mat-badge-content.mat-badge-active {
  transform: none;
}

.mat-badge-small .mat-badge-content {
  width: var(--mat-badge-legacy-small-size-container-size, unset);
  height: var(--mat-badge-legacy-small-size-container-size, unset);
  min-width: var(--mat-badge-small-size-container-size, 6px);
  min-height: var(--mat-badge-small-size-container-size, 6px);
  line-height: var(--mat-badge-small-size-line-height, 6px);
  padding: var(--mat-badge-small-size-container-padding, 0);
  font-size: var(--mat-badge-small-size-text-size, 0);
  margin: var(--mat-badge-small-size-container-offset, -6px 0);
}
.mat-badge-small.mat-badge-overlap .mat-badge-content {
  margin: var(--mat-badge-small-size-container-overlap-offset, -6px);
}

.mat-badge-medium .mat-badge-content {
  width: var(--mat-badge-legacy-container-size, unset);
  height: var(--mat-badge-legacy-container-size, unset);
  min-width: var(--mat-badge-container-size, 16px);
  min-height: var(--mat-badge-container-size, 16px);
  line-height: var(--mat-badge-line-height, 16px);
  padding: var(--mat-badge-container-padding, 0 4px);
  font-size: var(--mat-badge-text-size, var(--mat-sys-label-small-size));
  margin: var(--mat-badge-container-offset, -12px 0);
}
.mat-badge-medium.mat-badge-overlap .mat-badge-content {
  margin: var(--mat-badge-container-overlap-offset, -12px);
}

.mat-badge-large .mat-badge-content {
  width: var(--mat-badge-legacy-large-size-container-size, unset);
  height: var(--mat-badge-legacy-large-size-container-size, unset);
  min-width: var(--mat-badge-large-size-container-size, 16px);
  min-height: var(--mat-badge-large-size-container-size, 16px);
  line-height: var(--mat-badge-large-size-line-height, 16px);
  padding: var(--mat-badge-large-size-container-padding, 0 4px);
  font-size: var(--mat-badge-large-size-text-size, var(--mat-sys-label-small-size));
  margin: var(--mat-badge-large-size-container-offset, -12px 0);
}
.mat-badge-large.mat-badge-overlap .mat-badge-content {
  margin: var(--mat-badge-large-size-container-overlap-offset, -12px);
}
`],encapsulation:2,changeDetection:0})}return t})(),cM=(()=>{class t{_ngZone=d(j);_elementRef=d(P);_ariaDescriber=d(jf);_renderer=d(we);_animationsDisabled=ke();_idGenerator=d(Ie);get color(){return this._color}set color(e){this._setColor(e),this._color=e}_color="primary";overlap=!0;disabled=!1;position="above after";get content(){return this._content}set content(e){this._updateRenderedContent(e)}_content;get description(){return this._description}set description(e){this._updateDescription(e)}_description;size="medium";hidden=!1;_badgeElement;_inlineBadgeDescription;_isInitialized=!1;_interactivityChecker=d(Pa);_document=d(K);constructor(){let e=d(qe);e.load(cz),e.load(er)}isAbove(){return this.position.indexOf("below")===-1}isAfter(){return this.position.indexOf("before")===-1}getBadgeElement(){return this._badgeElement}ngOnInit(){this._clearExistingBadges(),this.content&&!this._badgeElement&&(this._badgeElement=this._createBadgeElement(),this._updateRenderedContent(this.content)),this._isInitialized=!0}ngAfterViewInit(){}ngOnDestroy(){this._renderer.destroyNode&&(this._renderer.destroyNode(this._badgeElement),this._inlineBadgeDescription?.remove()),this._ariaDescriber.removeDescription(this._elementRef.nativeElement,this.description)}_isHostInteractive(){return this._interactivityChecker.isFocusable(this._elementRef.nativeElement,{ignoreVisibility:!0})}_createBadgeElement(){let e=this._renderer.createElement("span"),i="mat-badge-active";return e.setAttribute("id",this._idGenerator.getId("mat-badge-content-")),e.setAttribute("aria-hidden","true"),e.classList.add(aM),this._animationsDisabled&&e.classList.add("_mat-animation-noopable"),this._elementRef.nativeElement.appendChild(e),typeof requestAnimationFrame=="function"&&!this._animationsDisabled?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>{e.classList.add(i)})}):e.classList.add(i),e}_updateRenderedContent(e){let i=`${e??""}`.trim();this._isInitialized&&i&&!this._badgeElement&&(this._badgeElement=this._createBadgeElement()),this._badgeElement&&(this._badgeElement.textContent=i),this._content=i}_updateDescription(e){this._ariaDescriber.removeDescription(this._elementRef.nativeElement,this.description),(!e||this._isHostInteractive())&&this._removeInlineDescription(),this._description=e,this._isHostInteractive()?this._ariaDescriber.describe(this._elementRef.nativeElement,e):this._updateInlineDescription()}_updateInlineDescription(){this._inlineBadgeDescription||(this._inlineBadgeDescription=this._document.createElement("span"),this._inlineBadgeDescription.classList.add("cdk-visually-hidden")),this._inlineBadgeDescription.textContent=this.description,this._badgeElement?.appendChild(this._inlineBadgeDescription)}_removeInlineDescription(){this._inlineBadgeDescription?.remove(),this._inlineBadgeDescription=void 0}_setColor(e){let i=this._elementRef.nativeElement.classList;i.remove(`mat-badge-${this._color}`),e&&i.add(`mat-badge-${e}`)}_clearExistingBadges(){let e=this._elementRef.nativeElement.querySelectorAll(`:scope > .${aM}`);for(let i of Array.from(e))i!==this._badgeElement&&i.remove()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=T({type:t,selectors:[["","matBadge",""]],hostAttrs:[1,"mat-badge"],hostVars:20,hostBindings:function(i,r){i&2&&z("mat-badge-overlap",r.overlap)("mat-badge-above",r.isAbove())("mat-badge-below",!r.isAbove())("mat-badge-before",!r.isAfter())("mat-badge-after",r.isAfter())("mat-badge-small",r.size==="small")("mat-badge-medium",r.size==="medium")("mat-badge-large",r.size==="large")("mat-badge-hidden",r.hidden||!r.content)("mat-badge-disabled",r.disabled)},inputs:{color:[0,"matBadgeColor","color"],overlap:[2,"matBadgeOverlap","overlap",V],disabled:[2,"matBadgeDisabled","disabled",V],position:[0,"matBadgePosition","position"],content:[0,"matBadge","content"],description:[0,"matBadgeDescription","description"],size:[0,"matBadgeSize","size"],hidden:[2,"matBadgeHidden","hidden",V]}})}return t})();function lz(t,n){t&1&&zt(0,"div",2)}var dz=new y("MAT_PROGRESS_BAR_DEFAULT_OPTIONS");var dM=(()=>{class t{_elementRef=d(P);_ngZone=d(j);_changeDetectorRef=d(ue);_renderer=d(we);_cleanupTransitionEnd;constructor(){let e=ql(),i=d(dz,{optional:!0});this._isNoopAnimation=e==="di-disabled",e==="reduced-motion"&&this._elementRef.nativeElement.classList.add("mat-progress-bar-reduced-motion"),i&&(i.color&&(this.color=this._defaultColor=i.color),this.mode=i.mode||this.mode)}_isNoopAnimation;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";get value(){return this._value}set value(e){this._value=lM(e||0),this._changeDetectorRef.markForCheck()}_value=0;get bufferValue(){return this._bufferValue||0}set bufferValue(e){this._bufferValue=lM(e||0),this._changeDetectorRef.markForCheck()}_bufferValue=0;animationEnd=new H;get mode(){return this._mode}set mode(e){this._mode=e,this._changeDetectorRef.markForCheck()}_mode="determinate";ngAfterViewInit(){this._ngZone.runOutsideAngular(()=>{this._cleanupTransitionEnd=this._renderer.listen(this._elementRef.nativeElement,"transitionend",this._transitionendHandler)})}ngOnDestroy(){this._cleanupTransitionEnd?.()}_getPrimaryBarTransform(){return`scaleX(${this._isIndeterminate()?1:this.value/100})`}_getBufferBarFlexBasis(){return`${this.mode==="buffer"?this.bufferValue:100}%`}_isIndeterminate(){return this.mode==="indeterminate"||this.mode==="query"}_transitionendHandler=e=>{this.animationEnd.observers.length===0||!e.target||!e.target.classList.contains("mdc-linear-progress__primary-bar")||(this.mode==="determinate"||this.mode==="buffer")&&this._ngZone.run(()=>this.animationEnd.next({value:this.value}))};static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-progress-bar"]],hostAttrs:["role","progressbar","aria-valuemin","0","aria-valuemax","100","tabindex","-1",1,"mat-mdc-progress-bar","mdc-linear-progress"],hostVars:10,hostBindings:function(i,r){i&2&&(Y("aria-valuenow",r._isIndeterminate()?null:r.value)("mode",r.mode),It("mat-"+r.color),z("_mat-animation-noopable",r._isNoopAnimation)("mdc-linear-progress--animation-ready",!r._isNoopAnimation)("mdc-linear-progress--indeterminate",r._isIndeterminate()))},inputs:{color:"color",value:[2,"value","value",ut],bufferValue:[2,"bufferValue","bufferValue",ut],mode:"mode"},outputs:{animationEnd:"animationEnd"},exportAs:["matProgressBar"],decls:7,vars:5,consts:[["aria-hidden","true",1,"mdc-linear-progress__buffer"],[1,"mdc-linear-progress__buffer-bar"],[1,"mdc-linear-progress__buffer-dots"],["aria-hidden","true",1,"mdc-linear-progress__bar","mdc-linear-progress__primary-bar"],[1,"mdc-linear-progress__bar-inner"],["aria-hidden","true",1,"mdc-linear-progress__bar","mdc-linear-progress__secondary-bar"]],template:function(i,r){i&1&&(Ne(0,"div",0),zt(1,"div",1),F(2,lz,1,0,"div",2),Ge(),Ne(3,"div",3),zt(4,"span",4),Ge(),Ne(5,"div",5),zt(6,"span",4),Ge()),i&2&&(p(),Qn("flex-basis",r._getBufferBarFlexBasis()),p(),L(r.mode==="buffer"?2:-1),p(),Qn("transform",r._getPrimaryBarTransform()))},styles:[`.mat-mdc-progress-bar {
  --mat-progress-bar-animation-multiplier: 1;
  display: block;
  text-align: start;
}
.mat-mdc-progress-bar[mode=query] {
  transform: scaleX(-1);
}
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__buffer-dots,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__primary-bar,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__secondary-bar,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__bar-inner.mdc-linear-progress__bar-inner {
  animation: none;
}
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__primary-bar,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__buffer-bar {
  transition: transform 1ms;
}

.mat-progress-bar-reduced-motion {
  --mat-progress-bar-animation-multiplier: 2;
}

.mdc-linear-progress {
  position: relative;
  width: 100%;
  transform: translateZ(0);
  outline: 1px solid transparent;
  overflow-x: hidden;
  transition: opacity 250ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  height: max(var(--mat-progress-bar-track-height, 4px), var(--mat-progress-bar-active-indicator-height, 4px));
}
@media (forced-colors: active) {
  .mdc-linear-progress {
    outline-color: CanvasText;
  }
}

.mdc-linear-progress__bar {
  position: absolute;
  top: 0;
  bottom: 0;
  margin: auto 0;
  width: 100%;
  animation: none;
  transform-origin: top left;
  transition: transform 250ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  height: var(--mat-progress-bar-active-indicator-height, 4px);
}
.mdc-linear-progress--indeterminate .mdc-linear-progress__bar {
  transition: none;
}
[dir=rtl] .mdc-linear-progress__bar {
  right: 0;
  transform-origin: center right;
}

.mdc-linear-progress__bar-inner {
  display: inline-block;
  position: absolute;
  width: 100%;
  animation: none;
  border-top-style: solid;
  border-color: var(--mat-progress-bar-active-indicator-color, var(--mat-sys-primary));
  border-top-width: var(--mat-progress-bar-active-indicator-height, 4px);
}

.mdc-linear-progress__buffer {
  display: flex;
  position: absolute;
  top: 0;
  bottom: 0;
  margin: auto 0;
  width: 100%;
  overflow: hidden;
  height: var(--mat-progress-bar-track-height, 4px);
  border-radius: var(--mat-progress-bar-track-shape, var(--mat-sys-corner-none));
}

.mdc-linear-progress__buffer-dots {
  background-image: radial-gradient(circle, var(--mat-progress-bar-track-color, var(--mat-sys-surface-variant)) calc(var(--mat-progress-bar-track-height, 4px) / 2), transparent 0);
  background-repeat: repeat-x;
  background-size: calc(calc(var(--mat-progress-bar-track-height, 4px) / 2) * 5);
  background-position: left;
  flex: auto;
  transform: rotate(180deg);
  animation: mdc-linear-progress-buffering calc(250ms * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
@media (forced-colors: active) {
  .mdc-linear-progress__buffer-dots {
    background-color: ButtonBorder;
  }
}
[dir=rtl] .mdc-linear-progress__buffer-dots {
  animation: mdc-linear-progress-buffering-reverse calc(250ms * var(--mat-progress-bar-animation-multiplier)) infinite linear;
  transform: rotate(0);
}

.mdc-linear-progress__buffer-bar {
  flex: 0 1 100%;
  transition: flex-basis 250ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  background-color: var(--mat-progress-bar-track-color, var(--mat-sys-surface-variant));
}

.mdc-linear-progress__primary-bar {
  transform: scaleX(0);
}
.mdc-linear-progress--indeterminate .mdc-linear-progress__primary-bar {
  left: -145.166611%;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__primary-bar {
  animation: mdc-linear-progress-primary-indeterminate-translate calc(2s * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__primary-bar > .mdc-linear-progress__bar-inner {
  animation: mdc-linear-progress-primary-indeterminate-scale calc(2s * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--animation-ready .mdc-linear-progress__primary-bar {
  animation-name: mdc-linear-progress-primary-indeterminate-translate-reverse;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--indeterminate .mdc-linear-progress__primary-bar {
  right: -145.166611%;
  left: auto;
}

.mdc-linear-progress__secondary-bar {
  display: none;
}
.mdc-linear-progress--indeterminate .mdc-linear-progress__secondary-bar {
  left: -54.888891%;
  display: block;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__secondary-bar {
  animation: mdc-linear-progress-secondary-indeterminate-translate calc(2s * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__secondary-bar > .mdc-linear-progress__bar-inner {
  animation: mdc-linear-progress-secondary-indeterminate-scale calc(2s * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--animation-ready .mdc-linear-progress__secondary-bar {
  animation-name: mdc-linear-progress-secondary-indeterminate-translate-reverse;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--indeterminate .mdc-linear-progress__secondary-bar {
  right: -54.888891%;
  left: auto;
}

@keyframes mdc-linear-progress-buffering {
  from {
    transform: rotate(180deg) translateX(calc(var(--mat-progress-bar-track-height, 4px) * -2.5));
  }
}
@keyframes mdc-linear-progress-primary-indeterminate-translate {
  0% {
    transform: translateX(0);
  }
  20% {
    animation-timing-function: cubic-bezier(0.5, 0, 0.701732, 0.495819);
    transform: translateX(0);
  }
  59.15% {
    animation-timing-function: cubic-bezier(0.302435, 0.381352, 0.55, 0.956352);
    transform: translateX(83.67142%);
  }
  100% {
    transform: translateX(200.611057%);
  }
}
@keyframes mdc-linear-progress-primary-indeterminate-scale {
  0% {
    transform: scaleX(0.08);
  }
  36.65% {
    animation-timing-function: cubic-bezier(0.334731, 0.12482, 0.785844, 1);
    transform: scaleX(0.08);
  }
  69.15% {
    animation-timing-function: cubic-bezier(0.06, 0.11, 0.6, 1);
    transform: scaleX(0.661479);
  }
  100% {
    transform: scaleX(0.08);
  }
}
@keyframes mdc-linear-progress-secondary-indeterminate-translate {
  0% {
    animation-timing-function: cubic-bezier(0.15, 0, 0.515058, 0.409685);
    transform: translateX(0);
  }
  25% {
    animation-timing-function: cubic-bezier(0.31033, 0.284058, 0.8, 0.733712);
    transform: translateX(37.651913%);
  }
  48.35% {
    animation-timing-function: cubic-bezier(0.4, 0.627035, 0.6, 0.902026);
    transform: translateX(84.386165%);
  }
  100% {
    transform: translateX(160.277782%);
  }
}
@keyframes mdc-linear-progress-secondary-indeterminate-scale {
  0% {
    animation-timing-function: cubic-bezier(0.205028, 0.057051, 0.57661, 0.453971);
    transform: scaleX(0.08);
  }
  19.15% {
    animation-timing-function: cubic-bezier(0.152313, 0.196432, 0.648374, 1.004315);
    transform: scaleX(0.457104);
  }
  44.15% {
    animation-timing-function: cubic-bezier(0.257759, -0.003163, 0.211762, 1.38179);
    transform: scaleX(0.72796);
  }
  100% {
    transform: scaleX(0.08);
  }
}
@keyframes mdc-linear-progress-primary-indeterminate-translate-reverse {
  0% {
    transform: translateX(0);
  }
  20% {
    animation-timing-function: cubic-bezier(0.5, 0, 0.701732, 0.495819);
    transform: translateX(0);
  }
  59.15% {
    animation-timing-function: cubic-bezier(0.302435, 0.381352, 0.55, 0.956352);
    transform: translateX(-83.67142%);
  }
  100% {
    transform: translateX(-200.611057%);
  }
}
@keyframes mdc-linear-progress-secondary-indeterminate-translate-reverse {
  0% {
    animation-timing-function: cubic-bezier(0.15, 0, 0.515058, 0.409685);
    transform: translateX(0);
  }
  25% {
    animation-timing-function: cubic-bezier(0.31033, 0.284058, 0.8, 0.733712);
    transform: translateX(-37.651913%);
  }
  48.35% {
    animation-timing-function: cubic-bezier(0.4, 0.627035, 0.6, 0.902026);
    transform: translateX(-84.386165%);
  }
  100% {
    transform: translateX(-160.277782%);
  }
}
@keyframes mdc-linear-progress-buffering-reverse {
  from {
    transform: translateX(-10px);
  }
}
`],encapsulation:2,changeDetection:0})}return t})();function lM(t,n=0,e=100){return Math.max(n,Math.min(e,t))}var uM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=W({type:t});static \u0275inj=G({imports:[ye]})}return t})();var mz=()=>({exact:!0});function fz(t,n){if(t&1){let e=dt();f(0,"button",28),R("click",function(){Me(e);let r=S();return Te(r.clearSearch())}),f(1,"mat-icon",12),v(2,"close"),h()()}}function hz(t,n){if(t&1&&(f(0,"button",14)(1,"mat-icon"),v(2,"person"),h()()),t&2){S();let e=Ve(17);D("matMenuTriggerFor",e)}}function pz(t,n){t&1&&(f(0,"button",15)(1,"a",29)(2,"mat-icon"),v(3,"personll"),h()()())}function gz(t,n){t&1&&(f(0,"div",30)(1,"a",31),v(2,"Profile"),h(),f(3,"a",31),v(4,"Orders"),h()())}function _z(t,n){t&1&&M(0,"mat-progress-bar",27)}var sp=class t{shopService=d(Ji);shopParams=new ao;search="";cartService=d(it);busyService=d(ac);accountS=d(Zt);router=d(Mt);isOpen=!1;OnSearchChange(){this.shopService.shopParams.search=this.search,console.log("search:",this.search),this.shopService.searchChanged.set(this.search),this.shopParams.pageNumber=1}clearSearch(){this.search="",this.shopService.searchChanged.set("")}logout(){localStorage.removeItem("token"),this.accountS.currentUser.set(null),this.router.navigateByUrl("/")}toggleMenu(){this.isOpen=!this.isOpen}onClick(n){n.target.closest(".dropdown")||(this.isOpen=!1)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-header"]],hostBindings:function(e,i){e&1&&R("click",function(o){return i.onClick(o)},j_)},decls:46,vars:9,consts:[["searchForm","ngForm"],["menu","matMenu"],[1,"shadow-md","p-3","w-full"],[1,"max-w-5xl","mx-auto","flex","flex-col","gap-3"],[1,"flex","items-center"],[1,"shrink-0"],["routerLink","/","src","/ProductImages/logo.png",1,"logo","max-h-16"],[1,"flex","items-center","gap-2","w-full","max-w-xl","mx-auto","flex-1","justify-center"],[1,"relative","flex","items-center","w-full","max-w-md","mx-4",3,"ngSubmit"],["type","button",1,"clearButton","absolute","right-15","top-5","-translate-y-1/2","text-gray-500","hover:text-black","mr-5"],["type","search","placeholder","Search products...","name","search",1,"text-sm","text-grey-900","border-black","border-2","p-2","w-full","max-w-md","rounded-4xl","px-4","focus:outline-none",3,"ngModelChange","ngModel"],["mat-icon-button","","type","submit",1,"Searchbutton","absolute","insert-y-0","right-11","flex","items-center","pl-3"],[1,""],[1,"flex","items-center","gap-3","shrink-0"],[1,"iconbutton",3,"matMenuTriggerFor"],[1,"iconbutton"],["mat-menu-item","","routerLink","/cart",1,"px-3"],["mat-menu-item","","routerLink","/orders",1,"px-3"],["mat-menu-item","",1,"px-3",3,"click"],["class","absolute right-0 mt-35 ml-32 bg-white shadow rounded w-40",4,"ngIf"],[1,"cart","shrink-0",3,"matBadge"],["routerLink","/cart","routerLinkActive","active"],[1,"flex","justify-center"],[1,"flex","gap-6","uppercase","text-lg"],["routerLink","/","routerLinkActive","active",3,"routerLinkActiveOptions"],["routerLink","/shop","routerLinkActive","active"],["href","#"],["mode","indeterminate"],["type","button",1,"clearButton","absolute","right-15","top-5","-translate-y-1/2","text-gray-500","hover:text-black","mr-5",3,"click"],["routerLink","/account/login"],[1,"absolute","right-0","mt-35","ml-32","bg-white","shadow","rounded","w-40"],[1,"block","px-4","py-2","hover:bg-gray-100"]],template:function(e,i){if(e&1){let r=dt();f(0,"header",2)(1,"div",3)(2,"div",4)(3,"div",5),M(4,"img",6),h(),f(5,"div",7)(6,"form",8,0),R("ngSubmit",function(){return i.OnSearchChange()}),F(8,fz,3,0,"button",9),f(9,"input",10),yr("ngModelChange",function(s){return Me(r),to(i.search,s)||(i.search=s),Te(s)}),h(),f(10,"button",11)(11,"mat-icon",12),v(12,"search"),h()()()(),f(13,"div",13),F(14,hz,3,1,"button",14)(15,pz,4,0,"button",15),f(16,"mat-menu",null,1)(18,"button",16)(19,"mat-icon"),v(20,"shopping_cart"),h(),v(21," My Cart "),h(),M(22,"mat-divider"),f(23,"button",17)(24,"mat-icon"),v(25,"shopping_cart"),h(),v(26," My Orders "),h(),M(27,"mat-divider"),f(28,"button",18),R("click",function(){return i.logout()}),f(29,"mat-icon"),v(30,"logout"),h(),v(31," Logout "),h()(),Be(32,gz,5,0,"div",19),f(33,"button",20)(34,"a",21)(35,"mat-icon"),v(36,"shopping_cart"),h()()()()(),f(37,"div",22)(38,"nav",23)(39,"a",24),v(40,"Home"),h(),f(41,"a",25),v(42,"Shop"),h(),f(43,"a",26),v(44,"Contact"),h()()()()(),F(45,_z,1,0,"mat-progress-bar",27)}e&2&&(p(8),L(i.search?8:-1),p(),br("ngModel",i.search),p(5),L(i.accountS.currentUser()?14:15),p(18),D("ngIf",i.isOpen),p(),D("matBadge",Xt(i.cartService.itemCount())),p(6),D("routerLinkActiveOptions",il(8,mz)),p(6),L(i.busyService.loading()?45:-1))},dependencies:[Uf,An,cM,ws,vo,Ya,ir,ri,Wa,Ts,Ms,nt,xb,uM,dM,$m,Av,gh,bo,Ha,hd],styles:[".iconbutton[_ngcontent-%COMP%]   .mat-icon[_ngcontent-%COMP%]{width:30px;height:28px;font-size:30px;line-height:33px}header[_ngcontent-%COMP%]{background:linear-gradient(180deg,#a647e9,#fff 99%);color:#000;padding:15px}.cart[_ngcontent-%COMP%]   .mat-icon[_ngcontent-%COMP%]{width:30px;height:28px;font-size:25px;line-height:33px}input[type=search][_ngcontent-%COMP%]::-webkit-search-cancel-button{display:none}.clearButton[_ngcontent-%COMP%]{transform:translateY(.5px) scale(.7)}a.active[_ngcontent-%COMP%]{color:#7d00fa}.iconbutton[_ngcontent-%COMP%]{cursor:pointer;border-radius:100%;align-items:center;transition:all .2s ease;width:35px;height:35px;min-width:35px}.iconbutton[_ngcontent-%COMP%]:hover{background-color:#0000001a}.iconbutton[_ngcontent-%COMP%]:active{background-color:#0006;transform:scale(.9)}"]})};var ap=class t{accountS=d(Zt);initService=d(sc);title="TechTronica";ngOnInit(){this.initService.init().subscribe()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-root"]],decls:3,vars:0,consts:[[1,"container","mt-6","mx-auto","max-w-5xl"]],template:function(e,i){e&1&&(M(0,"app-header"),f(1,"div",0),M(2,"router-outlet"),h())},dependencies:[Ol,sp],encapsulation:2})};Vv(ap,sM).catch(t=>console.error(t));
