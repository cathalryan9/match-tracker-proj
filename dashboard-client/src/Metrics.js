import React, {Component} from 'react';
class Metrics extends Component{
constructor(props){
super(props)
}


render(){
return <div><h1>{this.props.data}</h1></div>
}
}

export default Metrics;
